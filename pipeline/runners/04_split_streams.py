#!/usr/bin/env python
"""
Split video streams into individual files

Splits Videos into
- Left - Participant and
- Right - Interviewer
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

from rich.logging import RichHandler

from pipeline import orchestrator
from pipeline.core import split_streams
from pipeline.helpers import cli, utils

MODULE_NAME = "split-streams"

logger = logging.getLogger(MODULE_NAME)
logargs = {
    "level": logging.DEBUG,
    # "format": "%(asctime)s - %(process)d - %(name)s - %(levelname)s - %(message)s",
    "format": "%(message)s",
    "handlers": [RichHandler(rich_tracebacks=True)],
}
logging.basicConfig(**logargs)

console = utils.get_console()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog=MODULE_NAME,
        description="Module to split video file into individual streams.",
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
    zoom_meeting = config_params.get("zoom", "True")
    zoom_meeting = zoom_meeting.lower() == "true"

    COUNTER = 0
    STREAMS_COUNTER = 0

    logger.info("[bold green]Starting split streams loop...", extra={"markup": True})
    study_id = studies[0]
    logger.info(f"Starting with study: {study_id}", extra={"markup": True})

    while True:
        # Get file to process
        file_to_process = split_streams.get_file_to_process(
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

        video_path = Path(file_to_process[0])
        has_black_bars = bool(file_to_process[1])
        black_bar_height = file_to_process[2]
        if black_bar_height is not None:
            black_bar_height = int(black_bar_height)

        logger.info(
            f"[cyan]Splitting streams for {video_path}...",
            extra={"markup": True},
        )
        logger.info(
            f"Black bars detected: {has_black_bars}",
            extra={"markup": True},
        )
        logger.info(
            f"Black bar height: {black_bar_height}",
            extra={"markup": True},
        )

        streams = split_streams.split_streams(
            video_path=video_path,
            has_black_bars=has_black_bars,
            black_bar_height=black_bar_height,
            zoom_meeting=zoom_meeting,
            config_file=config_file,
        )
        STREAMS_COUNTER += len(streams)

        split_streams.log_streams(config_file=config_file, streams=streams)
