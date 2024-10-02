#!/usr/bin/env python
"""
Extracts audio streams from AV files.
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
from typing import List, Optional

from rich.logging import RichHandler

from pipeline import orchestrator
from pipeline.core import split_audio_streams
from pipeline.helpers import cli, utils, ffmpeg
from pipeline.helpers.timer import Timer
from pipeline.models.audio_streams import AudioStream

MODULE_NAME = "split-streams-audio"

logger = logging.getLogger(MODULE_NAME)
logargs = {
    "level": logging.DEBUG,
    # "format": "%(asctime)s - %(process)d - %(name)s - %(levelname)s - %(message)s",
    "format": "%(message)s",
    "handlers": [RichHandler(rich_tracebacks=True)],
}
logging.basicConfig(**logargs)

console = utils.get_console()

from pipeline.models import InterviewRole


# We expect 4 audio files:
#
# 1. left_right     ast_id = 1
# 2. subject        ast_id = 2
# 3. interviewer    ast_id = 3
# 4. webcam         ast_id = 4
#                   ast_id = 0 is reserved for unknown
#
# If there are less than 4 audio files, then pad with NULL
# and mark the role as TBD, for manual inspection.
#
# We also mark the ast_id as unknown, for manual inspection.
# ast_id = 0 is reserved for unknown
def get_role_from_idx(idx: int, num_audio_streams: int) -> Optional[InterviewRole]:
    """
    Get the InterviewRole of the person from the audio stream index.

    Args:
        idx (int): The index of the audio stream.
        num_audio_streams (int): The number of audio streams in the source file.

    Returns:
        Optional[InterviewRole]: The role of the person in the audio stream.
    """
    if num_audio_streams == 4:
        roles = [None, InterviewRole.SUBJECT, InterviewRole.INTERVIEWER, None]
    elif num_audio_streams == 3:
        roles = [InterviewRole.SUBJECT, InterviewRole.INTERVIEWER, None]
    else:
        return None

    return roles[idx]


def get_comment_from_idx(idx: int, num_audio_streams: int) -> str:
    """
    Get the comment for the audio stream index.

    Args:
        idx (int): The index of the audio stream.
        num_audio_streams (int): The number of audio streams in the source file.

    Returns:
        str: The comment for the audio stream.
    """
    if num_audio_streams == 4:
        ast_ids = ["leftright", "subject", "interviewer", "webcam"]
    elif num_audio_streams == 3:
        ast_ids = ["subject", "interviewer", "webcam"]
    else:
        return "unknown"

    return ast_ids[idx]


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

    COUNTER = 0
    STREAMS_COUNTER = 0

    logger.info("[bold green]Starting split streams loop...", extra={"markup": True})
    study_id = studies[0]
    logger.info(f"Starting with study: {study_id}", extra={"markup": True})

    while True:
        # Get file to process
        file_to_process = split_audio_streams.get_file_to_process(
            config_file=config_file, study_id=study_id
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

        source_path = Path(file_to_process)

        logger.info(f"Processing file: {source_path}", extra={"markup": True})
        audio_streams_count = split_audio_streams.get_audio_streams_count(
            source_path=source_path, config_file=config_file
        )
        logger.info(
            f"Found {audio_streams_count} audio streams in file: {source_path}",
            extra={"markup": True},
        )

        streams: List[AudioStream] = []
        with utils.get_progress_bar() as progress:
            task = progress.add_task(
                "Extracting audio streams", total=audio_streams_count
            )
            for idx in range(audio_streams_count):
                role = get_role_from_idx(idx, audio_streams_count)
                comment = get_comment_from_idx(idx, audio_streams_count)

                stream_path = split_audio_streams.construct_stream_path(
                    source_path=source_path, comment=comment, suffix="mp3"
                )

                with Timer() as timer:
                    ffmpeg.extract_audio_stream(
                        source_file=source_path,
                        output_file=stream_path,
                        stream_index=idx,
                        progress=progress,
                    )

                logger.info(
                    f"Extracted audio stream: {stream_path} ({timer.duration})",
                    extra={"markup": True},
                )

                stream = AudioStream(
                    as_source_path=source_path,
                    as_source_index=idx,
                    as_path=stream_path,
                    ir_role=role,
                    as_notes=comment,
                    as_process_time=timer.duration,
                )
                streams.append(stream)

                progress.update(task, advance=1)

        split_audio_streams.log_audio_streams(config_file=config_file, streams=streams)
        STREAMS_COUNTER += len(streams)
