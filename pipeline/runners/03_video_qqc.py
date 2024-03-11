#!/usr/bin/env python
"""
Run Quick QC on video files
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

from pipeline import core, orchestrator
from pipeline.core import video_qqc
from pipeline.helpers import cli, utils
from pipeline.helpers.timer import Timer
from pipeline.models.video_qqc import VideoQuickQc

MODULE_NAME = "video-qqc"

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
        prog=MODULE_NAME, description="Run Quick QC on video files."
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

    logger.info("[bold green]Starting video_qqc loop...", extra={"markup": True})
    study_id = studies[0]
    logger.info(f"Using study: {study_id}")

    while True:
        # Get file to process
        file_to_process = video_qqc.get_file_to_process(
            config_file=config_file, study_id=study_id
        )

        if file_to_process is None:
            if study_id == studies[-1]:
                # Log if any files were processed
                if COUNTER > 0:
                    core.log(
                        config_file=config_file,
                        module_name=MODULE_NAME,
                        message=f"Checked video_qqc for {COUNTER} files.",
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
        logger.info(
            f"[cyan]Checking video_qqc for {file_to_process}...",
            extra={"markup": True},
        )

        video_path = Path(file_to_process[0])
        duration = float(file_to_process[1])

        with Timer() as timer:
            qc_result: VideoQuickQc = video_qqc.do_video_qqc(
                video_path=video_path, duration=duration
            )

        # Add process time to qc_result
        qc_result.process_time = timer.duration

        video_qqc.log_video_qqc(
            config_file=config_file,
            result=qc_result,
        )
