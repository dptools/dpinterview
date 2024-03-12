#!/usr/bin/env python
"""
Runs OpenFace on video streams
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
from typing import Optional, Tuple

from rich.logging import RichHandler

from pipeline import orchestrator
from pipeline.core import openface
from pipeline.helpers import cli, utils
from pipeline.helpers.timer import Timer
from pipeline.models.interview_roles import InterviewRole

MODULE_NAME = "openface"
INSTANCE_NAME = MODULE_NAME

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
        prog="decryption", description="Module to decrypt files."
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

    orchestrator.redirect_temp_dir(config_file=config_file)

    console.rule(f"[bold red]{MODULE_NAME}")
    logger.info(f"Using config file: {config_file}")

    config_params = utils.config(config_file, section="general")
    studyies = orchestrator.get_studies(config_file=config_file)

    INSTANCE_NAME = utils.get_instance_name(
        module_name=INSTANCE_NAME, process_name=sys.argv[0]
    )

    COUNTER = 0
    SKIP_COUNTER = 0

    logger.info("[bold green]Starting OpenFace loop...", extra={"markup": True})
    study_id = studyies[0]
    logger.info(f"Staring with study: {study_id}")

    STASH: Optional[Tuple[Path, InterviewRole, Path, str]] = None

    while True:
        # Get file to process
        if STASH is not None:
            file_to_process = STASH
            STASH = None
        else:
            file_to_process = openface.get_file_to_process(
                config_file=config_file, study_id=study_id
            )

        if file_to_process is None:
            if study_id == studyies[-1]:
                logger.info("[bold green] No file to process.")
                openface.await_decrytion(
                    config_file=config_file, counter=COUNTER, module_name=MODULE_NAME
                )
                COUNTER = 0
                study_id = studyies[0]
                logger.info(f"Restarting with study: {study_id}")
                continue
            else:
                study_id = studyies[studyies.index(study_id) + 1]
                logger.info(f"[bold green]Switching to study: {study_id}")
                continue

        video_stream_path, interview_role, video_path, interview_name = file_to_process

        if not video_stream_path.exists():
            logger.error(f"Video stream path does not exist: {video_stream_path}")
            logger.error(f"video_path: {video_path}")
            sys.exit(1)

        openface_path = openface.construct_output_path(
            config_file=config_file, video_path=video_stream_path
        )
        logger.debug(f"Output path: {openface_path}")
        # Check if another process is running with same files
        if cli.check_if_running(
            process_name=str(video_stream_path)
        ) or cli.check_if_running(process_name=str(interview_name)):
            logger.warning(
                f"Another process is running with the same file: {video_stream_path}"
            )
            SKIP_COUNTER += 1
            if SKIP_COUNTER > orchestrator.get_max_instances(
                config_file=config_file,
                module_name=MODULE_NAME,
            ):
                console.log("[bold red]Max number of instances reached. Snoozing...")
                openface.await_decrytion(
                    config_file=config_file, counter=COUNTER, module_name=MODULE_NAME
                )
                SKIP_COUNTER = 0
                COUNTER = 0
                continue
            file_to_process = openface.get_file_to_process(
                config_file=config_file, study_id=study_id
            )
            continue
        else:
            SKIP_COUNTER = 0
            COUNTER += 1

        logger.info(
            f"Processing {video_stream_path} as {interview_role} from {video_path}"
        )

        try:
            # Run OpenFace
            with Timer() as timer:
                openface.run_openface(
                    config_file=config_file,
                    file_path_to_process=video_stream_path,
                    output_path=openface_path,
                )
            of_duration = timer.duration

            # Run OpenFace overlay (re-runs OpenFace on face-aligned frames)
            with Timer() as timer:
                openface.run_openface_overlay(
                    config_file=config_file,
                    openface_path=openface_path,
                    output_video_path=openface_path / "openface_aligned.mp4",
                    temp_dir_prefix=f"{interview_name}_",
                )
            overlay_duration = timer.duration
        except KeyboardInterrupt:
            logger.error("KeyboardInterrupt: Exiting...")
            logger.info("Cleaning up...")
            cli.remove_directory(
                path=openface_path,
            )
            sys.exit(1)

        # Log to DB
        openface.log_openface(
            config_file=config_file,
            video_stream_path=video_stream_path,
            interview_role=interview_role,
            video_path=video_path,
            openface_path=openface_path,
            openface_process_time=of_duration,
            overlay_process_time=overlay_duration,
        )

        openface.clean_up_after_openface(openface_path=openface_path)

        # Get other stream to process
        logger.info(f"Checking for other stream to process from {video_path}")
        STASH = openface.get_other_stream_to_process(
            config_file=config_file, video_path=video_path
        )

        if file_to_process is None:
            console.log("[bold green] No other stream to process.")
            continue
