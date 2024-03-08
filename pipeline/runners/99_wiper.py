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
from typing import List

from rich.logging import RichHandler

from pipeline import data
from pipeline.data import wipe
from pipeline.helpers import cli, db, utils
from pipeline.helpers.timer import Timer

MODULE_NAME = "pipeline.runners.99_wiper"

logger = logging.getLogger(MODULE_NAME)
logargs = {
    "level": logging.DEBUG,
    # "format": "%(asctime)s - %(process)d - %(name)s - %(levelname)s - %(message)s",
    "format": "%(message)s",
    "handlers": [RichHandler(rich_tracebacks=True)],
}
logging.basicConfig(**logargs)

console = utils.get_console()


def delete_files(files: List[Path], data_root: Path) -> None:
    """
    Delete files and folders passed as a list.
    If parent folder is empty, it will also be deleted.

    Args:
        files (List[Path]): List of files to delete
    """
    for file in files:
        try:
            if file.is_file():
                logger.debug(f"Deleting file: {file}")
                file.unlink()
            elif file.is_dir():
                logger.debug(f"Deleting directory: {file}")
                cli.remove_directory(path=file)

            parent_dir = file.parent
            while parent_dir != data_root:
                parent_files = list(parent_dir.iterdir())
                if not parent_files:
                    logger.debug(f"Deleting directory: {parent_dir}")
                    parent_dir.rmdir()
                    parent_dir = parent_dir.parent
                else:
                    break
        except FileNotFoundError:
            logger.warning(f"File not found: {file}. Skipping...")


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

    console.rule(f"[bold red]{MODULE_NAME}")
    logger.info(f"Using config file: {config_file}")

    config_params = utils.config(config_file, section="general")
    study_id = config_params["study"]
    data_root = Path(config_params["data_root"])

    COUNTER = 0

    logger.info("[bold green]Starting wipe loop...", extra={"markup": True})

    while True:
        # Get interview to wipe
        interview_to_wipe = wipe.get_interview_to_wipe(
            config_file=config_file, study_id=study_id
        )

        if interview_to_wipe is None:
            # Log if any interviews were wiped
            if COUNTER > 0:
                data.log(
                    config_file=config_file,
                    module_name=MODULE_NAME,
                    message=f"Wiped {COUNTER} interviews.",
                )
                COUNTER = 0

            # Exit if no interviews to wipe
            logger.info("No interviews to wipe.")
            sys.exit(0)

        COUNTER += 1
        logger.info(
            f"Wiping interview: [bold blue]{interview_to_wipe}",
            extra={"markup": True},
        )

        related_files = wipe.get_interview_files(
            config_file=config_file, interview_name=interview_to_wipe
        )
        drop_queries = wipe.drop_interview_queries(
            config_file=config_file, interview_name=interview_to_wipe
        )

        with Timer() as timer:
            # if not cli.confirm_action(
            #     f"Interview '{interview_to_wipe}' will be wiped. Do you want to continue?"
            # ):
            #     logger.info("Exiting...")
            #     sys.exit(0)
            delete_files(files=related_files, data_root=data_root)

            logger.info("Droping openface features...")
            wipe.drop_openface_features_query(
                config_file=config_file, interview_name=interview_to_wipe
            )
            db.execute_queries(
                config_file=config_file,
                queries=drop_queries,
                show_commands=True,
            )
        logger.info(
            f"Wiped interview: [bold blue]{interview_to_wipe} in {timer.duration} seconds.",
            extra={"markup": True},
        )
