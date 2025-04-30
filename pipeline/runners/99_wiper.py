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
    if parent.name == "dpinterview":
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

# from pipeline import data
from pipeline import orchestrator
from pipeline.core import wipe
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
            try:
                while parent_dir != data_root:
                    parent_files = list(parent_dir.iterdir())
                    if not parent_files:
                        logger.debug(f"Deleting directory: {parent_dir}")
                        parent_dir.rmdir()
                        parent_dir = parent_dir.parent
                    else:
                        break
            except PermissionError:
                logger.warning(f"Permission error: {parent_dir}. Skipping...")
        except FileNotFoundError:
            logger.warning(f"File not found: {file}. Skipping...")


def get_interviews_to_wipe(config_file: Path, study_id: str) -> List[str]:
    """
    Fetch a interview_name to delete from the database

    Args:
        config_file (Path): Path to config file
        study_id (str): Study ID

    Returns:
        str: interview_name
    """
    sql_query = f"""
        SELECT interviews.interview_name
        FROM decrypted_files
        INNER JOIN interview_files ON decrypted_files.source_path = interview_files.interview_file
        INNER JOIN interviews ON interview_files.interview_path = interviews.interview_path
        WHERE interviews.study_id = '{study_id}'
        ORDER BY interviews.interview_name ASC;
    """

    result = db.execute_sql(
        config_file=config_file,
        query=sql_query,
    )

    interviews = result["interview_name"].tolist()

    return interviews


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
    studies = orchestrator.get_studies(config_file=config_file)
    data_root = Path(config_params["data_root"])

    COUNTER = 0

    for study_id in studies:
        logger.info(f"Starting with study: {study_id}", extra={"markup": True})
        logger.info("[bold green]Starting wipe loop...", extra={"markup": True})

        interview_list = get_interviews_to_wipe(config_file=config_file, study_id=study_id)

        for interview_to_wipe in interview_list:
            # # Get interview to wipe
            # interview_to_wipe = wipe.get_interview_to_wipe(
            #     config_file=config_file, study_id=study_id
            # )

            # if interview_to_wipe is None:
            #     # Log if any interviews were wiped
            #     if COUNTER > 0:
            #         data.log(
            #             config_file=config_file,
            #             module_name=MODULE_NAME,
            #             message=f"Wiped {COUNTER} interviews.",
            #         )
            #         COUNTER = 0

            #     # Exit if no interviews to wipe
            #     logger.info("No interviews to wipe.")
            #     sys.exit(0)

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
                try:
                    db.execute_queries(
                        config_file=config_file,
                        queries=drop_queries,
                        show_commands=True,
                    )
                except Exception as e:
                    logger.error(f"Error: {e}")
                    logger.error("Continuing...")
            logger.info(
                f"Wiped interview: [bold blue]{interview_to_wipe} in {timer.duration} seconds.",
                extra={"markup": True},
            )

    wipe.wipe_all_interview_data(config_file=config_file)

    logger.info("Done.")
