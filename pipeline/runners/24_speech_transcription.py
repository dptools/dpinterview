#!/usr/bin/env python
"""
Transcribe individual audio files using WhisperNote (Whisper)
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
import json
import logging
from typing import Optional

from rich.logging import RichHandler

from pipeline import orchestrator
from pipeline.helpers import cli, db, dpdash, utils
from pipeline.helpers.timer import Timer
from pipeline.models.transcription import Transcription
from WhisperNote.whispernote import transcribe

MODULE_NAME = "transcription"

logger = logging.getLogger(MODULE_NAME)
logargs = {
    "level": logging.DEBUG,
    # "format": "%(asctime)s - %(process)d - %(name)s - %(levelname)s - %(message)s",
    "format": "%(message)s",
    "handlers": [RichHandler(rich_tracebacks=True)],
}
logging.basicConfig(**logargs)

console = utils.get_console()


def get_file_to_process(config_file: Path, study_id: str) -> Optional[Path]:
    """
    Fetch a file to process from the database.

    - Fetches a file that has not been processed yet and is part of the study.

    Args:
        config_file (Path): Path to config file

    Returns:
        Optional[Path]: Path to audio file
    """

    # Try to get audio streams from same interview that have been transcribed already
    sql_query = f"""
        SELECT ss_path
        FROM speech_streams
        LEFT JOIN speech_separation USING (as_path)
        LEFT JOIN audio_streams USING (as_path)
        LEFT JOIN decrypted_files ON audio_streams.as_source_path = decrypted_files.destination_path
        LEFT JOIN interview_files
            ON decrypted_files.source_path = interview_files.interview_file
        LEFT JOIN interviews
            ON interview_files.interview_path = interviews.interview_path
        WHERE interviews.study_id = '{study_id}' AND
            speech_streams.ss_path NOT IN (
                SELECT t_audio_path
                FROM transcription
            ) and speech_streams.as_path in (
                SELECT as_path
                from transcription
                left join speech_streams ON speech_streams.ss_path = transcription.t_audio_path
            )
        ORDER BY RANDOM()
        LIMIT 1;
        """

    audio_path = db.fetch_record(config_file=config_file, query=sql_query)

    if audio_path is None:
        # If no audio streams from the same interview have been transcribed, get any audio stream from the study
        sql_query = f"""
            SELECT ss_path
            FROM speech_streams
            LEFT JOIN speech_separation USING (as_path)
            LEFT JOIN audio_streams USING (as_path)
            LEFT JOIN decrypted_files ON audio_streams.as_source_path = decrypted_files.destination_path
            LEFT JOIN interview_files
                ON decrypted_files.source_path = interview_files.interview_file
            LEFT JOIN interviews
                ON interview_files.interview_path = interviews.interview_path
            WHERE interviews.study_id = '{study_id}' AND
                speech_streams.ss_path NOT IN (
                    SELECT t_audio_path
                    FROM transcription
                )
            ORDER BY RANDOM()
            LIMIT 1;
        """

        audio_path = db.fetch_record(config_file=config_file, query=sql_query)

    if audio_path is None:
        return None

    return Path(audio_path)


def construct_output_json_path(input_audio_file_path: Path, config_file: Path) -> Path:
    """
    Construct the output json file path, where the transcribed text will be saved.

    Args:
        input_audio_file_path (Path): Path to the input audio file
        config_file (Path): Path to config file
    """
    data_root = orchestrator.get_data_root(config_file=config_file)
    base_name = input_audio_file_path.name.split(".")[0]

    dp_dash_dict = dpdash.parse_dpdash_name(base_name)
    dp_data_type = dp_dash_dict["data_type"]

    if dp_data_type is None:
        raise ValueError(f"Could not parse data type from basename: {base_name}")

    data_type_parts = utils.camel_case_split(dp_data_type)  # type: ignore
    data_type = "_".join(data_type_parts)

    audio_transcripts_dir_dict = dp_dash_dict.copy()
    audio_transcripts_dir_dict["optional_tags"] = [
        tag
        for tag in audio_transcripts_dir_dict["optional_tags"]
        if "SPEAKER" not in tag
    ]
    audio_transcripts_dir = dpdash.get_dpdash_name_from_dict(audio_transcripts_dir_dict)

    output_path = Path(
        data_root,
        "PROTECTED",
        dp_dash_dict["study"],  # type: ignore
        dp_dash_dict["subject"],  # type: ignore
        data_type,  # type: ignore
        "processed",
        "transcripts",
        audio_transcripts_dir,
        f"{base_name}.json",
    )

    if not output_path.parent.exists():
        output_path.parent.mkdir(parents=True)

    return output_path


def log_transcription_to_db(
    transcription_result: Transcription, config_file: Path
) -> None:
    """
    Log the transcription result to the database.

    Args:
        transcription_result (Transcription): Transcription object
        config_file (Path): Path to config file

    Returns:
        None
    """
    sql_query = transcription_result.to_sql()
    db.execute_queries(config_file=config_file, queries=[sql_query])


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog=MODULE_NAME,
        description="Module to transcribe audio files.",
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

    transcription_params = utils.config(config_file, section="transcription")
    whisper_model = transcription_params.get("whisper_model", "large")
    logger.info(f"Using Whisper model: {whisper_model}")

    COUNTER = 0

    logger.info(
        "[bold green]Starting audio transcription loop...", extra={"markup": True}
    )
    study_id = studies[0]
    logger.info(f"Starting with study: {study_id}", extra={"markup": True})

    while True:
        # Get file to process
        file_to_process = get_file_to_process(
            config_file=config_file, study_id=study_id
        )

        if file_to_process is None:
            if study_id == studies[-1]:
                # Log if any files were processed
                if COUNTER > 0:
                    orchestrator.log(
                        config_file=config_file,
                        module_name=MODULE_NAME,
                        message=f"Transcribed {COUNTER} audio files.",
                    )
                    COUNTER = 0

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
            transcrption_results = transcribe.transcribe(
                input_audio_file_path=str(file_to_process),
                language="en",
                model=whisper_model,
                condition_on_previous_text=False,
            )

            output_json_path = construct_output_json_path(
                input_audio_file_path=file_to_process, config_file=config_file
            )

            with open(output_json_path, "w", encoding="utf-8") as output_file:
                json.dump(transcrption_results, output_file, indent=4)

        if timer.duration is None:
            timer.duration = 0

        logger.info(f"Transcription took: {timer.duration} seconds")
        logger.info(f"Transcription saved to: {output_json_path}")
        transcription_result = Transcription(
            t_audio_path=file_to_process,
            t_transcript_json_path=output_json_path,
            t_model=f"whisper:{whisper_model}",
            t_process_time=timer.duration,
        )

        log_transcription_to_db(
            transcription_result=transcription_result, config_file=config_file
        )
