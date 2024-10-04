#!/usr/bin/env python
"""
Separates / Isolates individual speakers from an audio stream.

This script uses the `pyannote/speech-separation-ami-1.0` model to separate
individual speakers from an audio stream.

Reference:
https://huggingface.co/pyannote/speech-separation-ami-1.0
"""
import sys
from pathlib import Path

file = Path(__file__).resolve()
parent = file.parent
ROOT = None
for parent in file.parents:
    if parent.name == "av-pipeline-v2":
        ROOT = parent
sys.path.append(str(ROOT))

# remove current directory from path
try:
    sys.path.remove(str(parent))
except ValueError:
    pass

import argparse
import logging
from typing import List, Optional, Tuple

import pyannote.core
import scipy.io.wavfile
import torchaudio
from pyannote.audio import Pipeline
from pyannote.audio.pipelines.utils.hook import ProgressHook
from rich.logging import RichHandler

from pipeline import orchestrator
from pipeline.helpers import cli, config, db, dpdash, utils
from pipeline.helpers.timer import Timer
from pipeline.models.speech_separation import SpeechSeparation
from pipeline.models.speech_streams import SpeechStream

MODULE_NAME = "speech-separation"

logger = logging.getLogger(MODULE_NAME)
logargs = {
    "level": logging.DEBUG,
    # "format": "%(asctime)s - %(process)d - %(name)s - %(levelname)s - %(message)s",
    "format": "%(message)s",
    "handlers": [RichHandler(rich_tracebacks=True)],
}
logging.basicConfig(**logargs)

noisy_modules: List[str] = [
    "urllib3.connectionpool",
    "fsspec.local",
    "speechbrain.utils.fetching",
    "speechbrain.utils.checkpoints",
    "speechbrain.utils.parameter_transfer",
]
utils.silence_logs(noisy_modules)

console = utils.get_console()


def get_file_to_process(
    config_file: Path, stream_idx: int, study_id: str
) -> Optional[Path]:
    """
    Fetch a file to process from the database.

    - Fetches a file that has not been processed yet and is part of the study.

    Args:
        config_file (Path): Path to config file
        stream_idx (int): Index of the audio stream to be fetched
        study_id (str): Study ID

    Returns:
        Optional[Path]: Path to audio file
    """

    sql_query = f"""
        SELECT as_path
        FROM audio_streams
        LEFT JOIN decrypted_files ON audio_streams.as_source_path = decrypted_files.destination_path
        LEFT JOIN interview_files
            ON decrypted_files.source_path = interview_files.interview_file
        LEFT JOIN interviews
            ON interview_files.interview_path = interviews.interview_path
        WHERE interviews.study_id = '{study_id}' AND
            as_source_index = {stream_idx} AND
            as_path NOT IN (
                SELECT as_path
                FROM speech_separation
            )
        ORDER BY RANDOM()
        LIMIT 1;
    """

    audio_path = db.fetch_record(config_file=config_file, query=sql_query)

    if audio_path is None:
        return None

    return Path(audio_path)


def separate_speech(
    audio_stream: Path,
    huggingface_auth_token: str,
) -> Tuple[
    pyannote.core.annotation.Annotation, pyannote.core.feature.SlidingWindowFeature
]:
    """
    Separate speech streams from an audio stream.

    Args:
        audio_stream (Path): Path to the audio stream

    Returns:
        Tuple[pyannote.core.annotation.Annotation, pyannote.core.feature.SlidingWindowFeature, int]:
            Tuple of speaker annotation, features
    """
    diarization_pipeline = "pyannote/speech-separation-ami-1.0"
    logger.info(f"Loading Speech Separation model ({diarization_pipeline})...")
    diarization_pipeline = Pipeline.from_pretrained(
        diarization_pipeline,
        use_auth_token=huggingface_auth_token,
    )

    logger.info(f"Separating speech streams from: {audio_stream}")
    with ProgressHook() as hook:
        diarization, sources = diarization_pipeline(
            audio_stream, hook=hook
        )

    return diarization, sources


def construct_diarization_rttm_file_name(audio_stream: Path) -> str:
    """
    Construct the RTTM file name for the diarization.

    Args:
        audio_stream (Path): Path to the audio stream

    Returns:
        file_name (str): RTTM file name
    """

    dpdash_dict = dpdash.parse_dpdash_name(audio_stream.name)
    dpdash_dict["optional_tags"] += ["diarization"]  # type: ignore

    file_name = dpdash.get_dpdash_name_from_dict(dpdash_dict)
    file_name = f"{file_name}.rttm"

    return file_name


def construct_diarization_rttm_file_path(audio_stream: Path) -> Path:
    """
    Construct the RTTM file path for the diarization.

    Args:
        audio_stream (Path): Path to the audio stream

    Returns:
        file_path (Path): Path to the RTTM file
    """

    rttm_file_name = construct_diarization_rttm_file_name(audio_stream)
    interview_processed_path = audio_stream.parent.parent.parent.parent

    rttm_file_path = interview_processed_path / "diarization" / rttm_file_name
    rttm_file_path.parent.mkdir(parents=True, exist_ok=True)

    return rttm_file_path


def write_diarization_rttm(
    diarization: pyannote.core.annotation.Annotation,
    output_path: Path,
) -> None:
    """
    Write the diarization annotation to an RTTM (Rich Transcription Time Marked) file.

    References:
    https://web.archive.org/web/20170119114252/http://www.itl.nist.gov/iad/mig/tests/rt/2009/docs/rt09-meeting-eval-plan-v2.pdf
    https://stackoverflow.com/questions/30975084/rttm-file-format

    Args:
        diarization (pyannote.core.annotation.Annotation): Diarization annotation
        output_path (Path): Path to the output RTTM file

    Returns:
        None
    """

    logger.info(f"Writing diarization to RTTM file: {output_path}")
    with open(output_path, "w", encoding="utf-8") as f:
        diarization.write_rttm(f)

    return None


def construct_speaker_streams_directory(audio_stream: Path) -> Path:
    """
    Construct the directory path for the speaker streams.

    Args:
        audio_stream (Path): Path to the audio stream

    Returns:
        path (Path): Path to the speaker streams directory
    """

    interview_streams_path = audio_stream.parent
    speaker_streams_path = interview_streams_path / "speaker_streams"

    speaker_streams_path.mkdir(parents=True, exist_ok=True)

    return speaker_streams_path


def construct_speaker_stream_file_name(audio_stream: Path, speaker_label: str) -> str:
    """
    Construct the speaker stream file name.

    Args:
        audio_stream (Path): Path to the audio stream
        speaker_label (str): Speaker label

    Returns:
        file_name (str): Speaker stream file name
    """
    # Remove '_' from speaker label
    speaker_label = speaker_label.replace("_", "")

    dpdash_dict = dpdash.parse_dpdash_name(audio_stream.name)
    dpdash_dict["optional_tags"] += [speaker_label]  # type: ignore

    file_name = dpdash.get_dpdash_name_from_dict(dpdash_dict)
    file_name = f"{file_name}.wav"

    return file_name


def construct_speaker_stream_file_path(audio_stream: Path, speaker_label: str) -> Path:
    """
    Construct the speaker stream file path.

    Args:
        audio_stream (Path): Path to the audio stream
        speaker_label (str): Speaker label

    Returns:
        file_path (Path): Path to the speaker stream file
    """

    speaker_streams_dir = construct_speaker_streams_directory(audio_stream)
    speaker_stream_file_name = construct_speaker_stream_file_name(
        audio_stream, speaker_label
    )

    speaker_stream_file_path = speaker_streams_dir / speaker_stream_file_name

    return speaker_stream_file_path


def write_speaker_streams(
    audio_stream: Path,
    diarization: pyannote.core.annotation.Annotation,
    sources: pyannote.core.feature.SlidingWindowFeature,
) -> List[SpeechStream]:
    """
    Write the speaker streams to the output directory.

    Args:
        audio_stream (Path): Path to the audio stream
        sources (pyannote.core.feature.SlidingWindowFeature): Speaker features
        output_dir (Path): Path to the output directory

    Returns:
        List[SpeechStream]: List of speaker streams
    """

    speech_streams: List[SpeechStream] = []

    for s_idx, speaker_label in enumerate(diarization.labels()):
        speaker_stream_file_path = construct_speaker_stream_file_path(
            audio_stream, str(speaker_label)
        )
        logger.info(f"Writing {speaker_label} stream to: {speaker_stream_file_path}...")
        # The model outputs in 16kHz sample rate, not native sample rate
        scipy.io.wavfile.write(
            speaker_stream_file_path, rate=16000, data=sources.data[:, s_idx]
        )
        speech_stream = SpeechStream(
            as_path=audio_stream,
            ss_speaker_label=str(speaker_label),
            ss_path=speaker_stream_file_path,
        )
        speech_streams.append(speech_stream)

    return speech_streams


def log_speaker_separated_streams(
    config_file: Path,
    speech_separation: SpeechSeparation,
    speech_streams: List[SpeechStream],
) -> None:
    """
    Log the speaker separated streams to the database.

    Args:
        config_file (Path): Path to the config file
        speech_separation (SpeechSeparation): Speech separation instance
        speech_streams (List[SpeechStream]): List of speaker streams

    Returns:
        None
    """

    queries: List[str] = []

    # Log speech separation
    speech_separation_query = speech_separation.to_sql()
    queries.append(speech_separation_query)

    # Log speaker streams
    for speech_stream in speech_streams:
        speech_stream_query = speech_stream.to_sql()
        queries.append(speech_stream_query)

    db.execute_queries(config_file=config_file, queries=queries)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog=MODULE_NAME,
        description="Module to split audio streams from AV files.",
    )
    parser.add_argument(
        "-c", "--config", type=str, help="Path to the config file.", required=False
    )

    args = parser.parse_args()

    # Check if parseer has config file
    if args.config:
        config_file = Path(args.config).resolve()
        if not config_file.exists():
            logger.error(f"Error: Config file '{config_file}' does not exist.")
            sys.exit(1)
    else:
        if cli.confirm_action("Using default config file."):
            config_file = utils.get_config_file_path()
        else:
            sys.exit(1)

    utils.configure_logging(
        config_file=config_file, module_name=MODULE_NAME, logger=logger
    )

    console.rule(f"[bold red]{MODULE_NAME}")
    logger.info(f"Using config file: {config_file}")

    config_params = utils.config(config_file, section="general")
    studies = orchestrator.get_studies(config_file=config_file)

    speech_separation_params = utils.config(config_file, section="speech_separation")
    audio_stream_idx = int(speech_separation_params["audio_stream_idx"])

    huggingface_auth_token = config.get_key_from_config_file(
        config_file=config_file, section="huggingface"
    )

    COUNTER = 0
    STREAMS_COUNTER = 0

    logger.info(
        "[bold green]Starting spekaer separation loop...", extra={"markup": True}
    )
    study_id = studies[0]
    logger.info(f"Starting with study: {study_id}", extra={"markup": True})

    while True:
        # Get file to process
        file_to_process = get_file_to_process(
            config_file=config_file, study_id=study_id, stream_idx=audio_stream_idx
        )

        if file_to_process is None:
            if study_id == studies[-1]:
                # Log if any files were processed
                if COUNTER > 0:
                    orchestrator.log(
                        config_file=config_file,
                        module_name=MODULE_NAME,
                        message=f"Split {COUNTER} files into {STREAMS_COUNTER} streams.",
                    )
                    COUNTER = 0
                    STREAMS_COUNTER = 0

                # Snooze if no files to process
                orchestrator.snooze(config_file=config_file)
                study_id = studies[0]
                logger.info(
                    f"Restarting with study: {study_id}", extra={"markup": True}
                )
                continue
            else:
                study_id = studies[studies.index(study_id) + 1]
                logger.info(f"Switching to study: {study_id}", extra={"markup": True})
                continue

        COUNTER += 1
        logger.info(f"Processing audio stream: {file_to_process}")

        with Timer() as timer:
            diarization, sources = separate_speech(
                audio_stream=file_to_process,
                huggingface_auth_token=huggingface_auth_token,
            )

            rttm_file_path = construct_diarization_rttm_file_path(file_to_process)
            write_diarization_rttm(diarization, rttm_file_path)

            speech_streams = write_speaker_streams(
                audio_stream=file_to_process,
                diarization=diarization,
                sources=sources,
            )
            logger.info(f"Identified {len(speech_streams)} speakers.")

        STREAMS_COUNTER += len(speech_streams)
        process_time = timer.duration

        logger.info(f"Processed {file_to_process} in {process_time:.2f} seconds.")

        speech_separation = SpeechSeparation(
            as_path=file_to_process,
            ss_diariazation_rttm_path=rttm_file_path,
            ss_identified_speakers_count=len(speech_streams),
            ss_process_time=process_time,
        )

        logger.info("Logging speech separation to database...")
        log_speaker_separated_streams(
            config_file=config_file,
            speech_separation=speech_separation,
            speech_streams=speech_streams,
        )
