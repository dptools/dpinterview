"""
SFTP Helper Functions
"""

import logging
from pathlib import Path
from typing import Dict

import paramiko

from pipeline.helpers import utils

logger = logging.getLogger(__name__)


def get_sftp_credentials(
    config_file: Path, sftp_credentials_name: str = "transcribeme_sftp"
) -> Dict[str, str]:
    """
    Retrieves the database credentials from the configuration file.

    Args:
        config_file (Path): The path to the configuration file.
        db (str, optional): The section of the configuration file to use.
            Defaults to "postgresql".

    Returns:
        Dict[str, str]: A dictionary containing the database credentials.
    """
    db_params = utils.config(path=config_file, section=sftp_credentials_name)

    if "key_file" in db_params:
        key_file = Path(db_params["key_file"])
        credentials = utils.config(path=key_file, section=sftp_credentials_name)
    else:
        credentials = db_params

    return credentials


def sftp_upload_file(
    sftp: paramiko.SFTPClient,
    local_path: Path,
    remote_path: Path,
) -> None:
    """
    Uploads a file to a remote SFTP server.

    Args:
        sftp (paramiko.SFTPClient): The SFTP client object.
        local_path (Path): The path to the file on the local machine.
        remote_path (Path): The path where the file will be uploaded on the remote server.
    """
    logger.info(f"Uploading [local] {local_path} to [remote] {remote_path}")
    sftp.put(str(local_path), str(remote_path))


def sftp_download_file(
    sftp: paramiko.SFTPClient,
    remote_path: Path,
    local_path: Path,
) -> None:
    """
    Downloads a file from a remote SFTP server to a local path.

    Args:
        sftp (paramiko.SFTPClient): The SFTP client object.
        remote_path (Path): The path to the file on the remote server.
        local_path (Path): The path where the file will be saved locally.
    """
    logger.info(f"Downloading [remote] {remote_path} to [local] {local_path}")
    sftp.get(str(remote_path), str(local_path))


def sftp_move_file(
    sftp: paramiko.SFTPClient,
    remote_source_path: Path,
    remote_destination_path: Path,
) -> None:
    """
    Moves a file from one location to another on the remote SFTP server.

    Args:
        sftp (paramiko.SFTPClient): The SFTP client object.
        remote_source_path (Path): The source path of the file on the remote server.
        remote_destination_path (Path): The destination path for the file on the remote server.
    """
    logger.info(
        f"Moving [remote] {remote_source_path} to [remote] {remote_destination_path}"
    )
    try:
        sftp.rename(str(remote_source_path), str(remote_destination_path))
    except OSError as e:
        # Create the destination directory if it does not exist
        if e.errno == 2:
            logger.info(
                f"Destination directory does not exist, creating: {remote_destination_path.parent}"
            )
        sftp.mkdir(str(remote_destination_path.parent))
        # Retry the rename operation after creating the directory
        sftp.rename(str(remote_source_path), str(remote_destination_path))


def sftp_delete_file(
    sftp: paramiko.SFTPClient,
    remote_path: Path,
) -> None:
    """
    Deletes a file from the remote SFTP server.

    Args:
        sftp (paramiko.SFTPClient): The SFTP client object.
        remote_path (Path): The path to the file on the remote server.
    """
    logger.info(f"Deleting [remote] {remote_path}")
    sftp.remove(str(remote_path))
