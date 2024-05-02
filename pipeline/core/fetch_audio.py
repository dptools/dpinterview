"""
Helper functions for fetching audio files to decrypt.
"""

import logging
from pathlib import Path
from typing import Optional, Tuple, Callable

from pipeline.helpers import db, dpdash
from pipeline.core import fetch_video

logger = logging.getLogger(__name__)


def get_file_to_decrypt(
    config_file: Path, study_id: str
) -> Optional[Tuple[str, str, str, str]]:
    """
    Retrieves a file to decrypt from the database.

    Args:
        config_file (Path): The path to the config file.

    Returns:
        Optional[Tuple[str, str, str, str]]: A tuple containing the path to the file to decrypt,
            the interview type, the interview name, and the interview tags.
    """

    query = f"""
        SELECT interview_file, interview_type, interview_name, interview_file_tags
        FROM interview_files
        INNER JOIN interviews ON interview_files.interview_path = interviews.interview_path
        WHERE interviews.study_id = '{study_id}' AND
            interview_files.interview_file_tags LIKE '%%audio%%' AND
            interview_files.interview_file NOT IN (
                SELECT source_path FROM decrypted_files
            ) AND
            interview_files.ignored = FALSE
        ORDER BY RANDOM()
        LIMIT 1
        """

    df = db.execute_sql(config_file=config_file, query=query)

    if df.empty:
        return None

    file_to_decrypt = df["interview_file"].iloc[0]
    interview_type = df["interview_type"].iloc[0]
    interview_name = df["interview_name"].iloc[0]
    interview_tags = df["interview_file_tags"].iloc[0]

    return file_to_decrypt, interview_type, interview_name, interview_tags


def construct_dest_dir(
    encrypted_file_path: Path, interview_type: str, study_id: str, data_root: Path
) -> Path:
    """
    Constructs the destination directory for the decrypted file.

    Args:
        encrypted_file_path (str): The path to the encrypted file.
        study_id (str): The ID of the study.
        data_root (str): The root directory of the data.

    Returns:
        str: The destination directory for the decrypted file.
    """
    return fetch_video.construct_dest_dir(
        encrypted_file_path, interview_type, study_id, data_root
    )


def construct_dest_file_name(
    file_to_decrypt: Path, interview_name: str, interview_file_tags: str
) -> str:
    """
    Constructs a dpdash compliant  destination file name for the decrypted file.

    Args:
        file_to_decrypt (str): The path to the file to decrypt.
        interview_name (str): The name of the interview.
    """
    ext = file_to_decrypt.suffixes[-2]
    dp_dash_dict = dpdash.parse_dpdash_name(interview_name)
    dp_dash_dict["category"] = "audio"

    if "interviewer" in interview_file_tags:
        dp_dash_dict["optional_tags"] = ["interviewer"]
    if "participant" in interview_file_tags:
        dp_dash_dict["optional_tags"] = ["subject"]
    if "combined" in interview_file_tags:
        dp_dash_dict["optional_tags"] = ["combined"]

    dest_file_name = dpdash.get_dpdash_name_from_dict(dp_dash_dict)
    dest_file_name = f"{dest_file_name}{ext}"

    return dest_file_name


def reconstruct_dest_file_name(dest_file_name: str, suffix: str) -> str:
    """
    Adds a suffix to the destination file name.

    Handles the case where the destination file name already has a suffix.

    Args:
        dest_file_name (str): The destination file name.
        suffix (str): The suffix to add.
    """
    return fetch_video.reconstruct_dest_file_name(dest_file_name, suffix)


def log_decryption_request(
    config_file: Path,
    source_path: Path,
    destination_path: Path,
    requested_by: str,
    on_failure: Callable,
) -> None:
    """
    Logs the request to decrypt a file.

    Args:
        config_file (Path): The path to the configuration file.
        source_path (str): The path to the file before decryption.
        destination_path (str): The path to the file after decryption.
        requested_by (str): The name of the process requesting the decryption.

    Returns:
        None
    """
    fetch_video.log_decryption_request(
        config_file, source_path, destination_path, requested_by, on_failure
    )
