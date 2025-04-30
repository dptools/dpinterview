#!/usr/bin/env python
"""
Upload PDF reports to Dropbox.
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
import os
import time
from datetime import datetime
from typing import Any, List
import dropbox

from rich.logging import RichHandler

from pipeline.helpers import dropbox as dropbox_helper
from pipeline.helpers import cli, db, utils

MODULE_NAME = "pipeline.runners.71_dropbox_sync"
logger = logging.getLogger(MODULE_NAME)
logargs = {
    "level": logging.DEBUG,
    # "format": "%(asctime)s - %(process)d - %(name)s - %(levelname)s - %(message)s",
    "format": "%(message)s",
    "handlers": [RichHandler(rich_tracebacks=True)],
}
logging.basicConfig(**logargs)

console = utils.get_console()

# Silence logs from other modules
noisy_modules: List[str] = [
    "urllib3.connectionpool",
    "dropbox",
]
for module in noisy_modules:
    logger.debug(f"Setting log level for {module} to INFO")
    logging.getLogger(module).setLevel(logging.INFO)


def get_reports_from_db(config_file: Path, verison: str = "v1.0.0"):
    """
    Get all generated reports from the database.

    Args:
        config_file (Path): The path to the config file.
        verison (str): The version of the reports to get.

    Returns:
        pd.DataFrame: The reports.
    """
    sql = f"""
        SELECT *
        FROM pdf_reports
        WHERE pr_version = '{verison}'
        ORDER BY interview_name;
    """

    df = db.execute_sql(config_file=config_file, query=sql)

    return df


def get_files_in_dropbox(config_file: Path) -> List[Any]:
    """
    Get all files in the Dropbox reports folder. This is used to skip
    uploading reports that are already in Dropbox.

    Args:
        config_file (Path): The path to the config file.

    Returns:
        List[Any]: The files in the Dropbox reports folder.
    """
    dbx = dropbox_helper.get_dropbox_client(config_file)

    dropbox_params = utils.config(path=config_file, section="dropbox")
    dropbox_folder = dropbox_params["reports_folder"]

    return dropbox_helper.get_files(dbx, dropbox_folder)


def get_files_to_upload(config_file: Path) -> List[Path]:
    """
    Get the files to upload to Dropbox, by comparing the reports in the
    database with the reports in Dropbox.

    Skip reports that are already in Dropbox and are newer than the ones
    in the database.

    Args:
        config_file (Path): The path to the config file.

    Returns:
        List[Path]: The files to upload to Dropbox.
    """
    files_to_upload: List[Path] = []

    df = get_reports_from_db(config_file=config_file)
    dbx_files = get_files_in_dropbox(config_file=config_file)

    for _, row in df.iterrows():
        path = row["pr_path"]
        basename = os.path.basename(path)

        # Check if basename exists in dropbox folder
        if basename in [entry.name for entry in dbx_files]:
            metadata = dbx_files[[entry.name for entry in dbx_files].index(basename)]

            # Check if current file is newer than the one in dropbox
            mtime = os.path.getmtime(path)
            mtime_dt = datetime(*time.gmtime(mtime)[:6])
            size = os.path.getsize(path)
            if (
                isinstance(metadata, dropbox.files.FileMetadata)  # type: ignore
                and mtime_dt == metadata.client_modified
                and size == metadata.size
            ):
                pass
            else:
                files_to_upload.append(path)
            continue

        files_to_upload.append(path)

    files_to_upload = [Path(path) for path in files_to_upload]
    checked_files: List[Path] = []
    for file in files_to_upload:
        if not file.exists():
            logger.warning(f"File {file} does not exist. Removing from list.")
            continue
        checked_files.append(file)

    return checked_files


def main(config_file: Path):
    """
    Upload PDF reports to Dropbox.

    Args:
        config_file (Path): The path to the config file.

    Returns:
        None
    """
    logger.info("Getting files to upload...")
    files_to_upload = get_files_to_upload(config_file=config_file)

    if not files_to_upload:
        logger.warning("No files to upload.")
        return

    dbx = dropbox_helper.get_dropbox_client(config_file)

    dropbox_params = utils.config(path=config_file, section="dropbox")
    dropbox_folder = dropbox_params["reports_folder"]

    logger.info("Uploading files to Dropbox...")
    logger.info(f"Dropbox folder: {dropbox_folder}")
    dropbox_helper.upload_files(dbx, files_to_upload, dropbox_folder)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="dropbox sync", description="Module to sync PDF reports to Dropbox."
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

    main(config_file=config_file)
