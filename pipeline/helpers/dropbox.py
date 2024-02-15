"""
Helper functions for Dropbox integration.
"""

import logging
import os
import time
from datetime import datetime
from pathlib import Path
from typing import Any, List

import dropbox

from pipeline.helpers import utils

logger = logging.getLogger("pipeline.helpers.dropbox")


def get_dropbox_client(config_file: Path) -> dropbox.Dropbox:
    """
    Get Dropbox client using the key file from the config file.

    Args:
        config_file (Path): Path to the config file.

    Returns:
        dropbox.Dropbox: Dropbox client.
    """
    params = utils.config(path=config_file, section="dropbox")
    key_file = params["key_file"]
    key = open(key_file, "r", encoding="utf-8").read().strip()

    dbx = dropbox.Dropbox(key)

    return dbx


def get_files(client: dropbox.Dropbox, dropbox_directory: str) -> List[Any]:
    """
    Get all files in the Dropbox directory.

    Args:
        dbx (dropbox.Dropbox): Dropbox client.
        dropbox_directory (str): The Dropbox directory.

    Returns:
        List[Any]: The files in the Dropbox directory.
    """

    dbx_files = []
    result = client.files_list_folder(dropbox_directory)
    dbx_files.extend(result.entries)  # type: ignore

    while result.has_more:  # type: ignore
        logger.info("More files in dropbox. Getting next batch...")
        result = client.files_list_folder_continue(result.cursor)  # type: ignore
        dbx_files.extend(result.entries)  # type: ignore

    return dbx_files


def upload_files(
    client: dropbox.Dropbox, files_to_upload: List[Path], dropbox_directory: str
) -> None:
    """
    Upload files to Dropbox.

    Args:
        dbx (dropbox.Dropbox): Dropbox client.
        files_to_upload (List[Path]): The files to upload.
        dropbox_directory (str): The Dropbox directory.

    Returns:
        None
    """
    with utils.get_progress_bar() as progress:
        task = progress.add_task("Uploading files...", total=len(files_to_upload))
        for file in files_to_upload:

            if not Path(file).exists():
                logger.warning(f"File {file} does not exist. Skipping...")
                raise FileNotFoundError(f"File {file} does not exist.")

            progress.update(task, advance=1)
            basename = os.path.basename(file)

            mtime = os.path.getmtime(file)
            mtime_dt = datetime(*time.gmtime(mtime)[:6])

            with open(file, "rb") as f:
                client.files_upload(
                    f.read(),
                    path=dropbox_directory + "/" + basename,
                    mode=dropbox.files.WriteMode.overwrite,  # type: ignore
                    client_modified=mtime_dt,
                )
