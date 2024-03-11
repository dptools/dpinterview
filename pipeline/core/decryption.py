"""
Helper functions for decrypting files.
"""

import logging
import sys
from pathlib import Path
from typing import List, Optional, Tuple

import cryptease as crypt

from pipeline.helpers import db, dpdash, utils
from pipeline.models.decrypted_files import DecryptedFile

logger = logging.getLogger(__name__)


def get_file_to_decrypt(config_file: Path) -> Optional[Tuple[str, str, str]]:
    """
    Retrieves a file to decrypt from the database.

    Args:
        config_file (Path): The path to the config file.

    Returns:
        Optional[Tuple[str, str, str]]: A tuple containing the path to the file to decrypt,
            the interview type, and the interview name.
    """
    config_params = utils.config(config_file, section="general")
    study_id = config_params["study"]

    query = f"""
        SELECT interview_file, interview_type, interview_name FROM interview_files
        INNER JOIN interviews ON interview_files.interview_path = interviews.interview_path
        WHERE interviews.study_id = '{study_id}' AND
            interview_files.interview_file_tags LIKE '%%video%%' AND
            interview_files.interview_file NOT IN (
                SELECT source_path FROM decrypted_files
            )
        ORDER BY RANDOM()
        LIMIT 1
        """

    df = db.execute_sql(config_file=config_file, query=query)

    if df.empty:
        return None

    file_to_decrypt = df["interview_file"].iloc[0]
    interview_type = df["interview_type"].iloc[0]
    interview_name = df["interview_name"].iloc[0]

    return file_to_decrypt, interview_type, interview_name


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
    # Get PARTICIPANT_ID and INTERVIEW_NAME from osir_audio_video_file_path
    # INTERVIEW_NAME = encrypted_file_path.split("/")[-2]
    participant_id = str(encrypted_file_path).split("/")[-5]

    destination_dir = Path(
        data_root,
        "PROTECTED",
        study_id,
        participant_id,
        f"{interview_type}_interview",
        "processed",
        "decrypted",
    )

    if not destination_dir.exists():
        destination_dir.mkdir(parents=True)

    return Path(destination_dir)


def construct_dest_file_name(file_to_decrypt: Path, interview_name: str) -> str:
    """
    Constructs a dpdash compliant  destination file name for the decrypted file.

    Args:
        file_to_decrypt (str): The path to the file to decrypt.
        interview_name (str): The name of the interview.
    """
    ext = file_to_decrypt.suffixes[-2]
    dp_dash_dict = dpdash.parse_dpdash_name(interview_name)
    dp_dash_dict["category"] = "audioVideo"

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
    ext = dest_file_name.split(".")[-1]
    file_name = dest_file_name.split(".")[0]

    dp_dash_dict = dpdash.parse_dpdash_name(file_name)

    if dp_dash_dict["optional_tags"] is None:
        dp_dash_dict["optional_tags"] = []

    optional_tags: List[str] = dp_dash_dict["optional_tags"]  # type: ignore
    optional_tags.append(suffix)
    dp_dash_dict["optional_tags"] = optional_tags

    new_name = dpdash.get_dpdash_name_from_dict(dp_dash_dict)
    new_name = f"{new_name}.{ext}"

    return new_name


def get_key_from_config_file(config_file: Path) -> str:
    """
    Retrieves the decryption key from the specified config file.

    Args:
        config_file (Path): The path to the config file.

    Returns:
        str: The decryption key.
    """
    # Get decryption key from config file
    params = utils.config(config_file, section="decryption")
    key_file = params["key_file"]
    logger.debug(f"Using decryption key from: {key_file}")

    # Check if key_file exists
    if not Path(key_file).exists():
        logger.error(f"Error: key_file '{key_file}' does not exist.")
        sys.exit(1)

    # Get key from key_file
    with open(key_file, "r", encoding="utf-8") as f:
        key = f.read().strip()

    return key


def decrypt_file(
    config_file: Path, file_to_decrypt: Path, path_for_decrypted_file: Path
) -> Path:
    """
    Decrypts a file using a key obtained from a configuration file.

    Args:
        config_file (str): The path to the configuration file containing the key.
        file_to_decrypt (str): The path to the file to be decrypted.
        path_for_decrypted_file (str): The path to save the decrypted file.

    Returns:
        None
    """
    # Decrypt file
    key = get_key_from_config_file(config_file=config_file)

    suffix: int = 1
    while path_for_decrypted_file.exists():
        logger.warning(
            f"Error: Decrypted file already exists: {path_for_decrypted_file}"
        )
        logger.warning(f"Appending suffix: {suffix}")
        dest_file_name = path_for_decrypted_file.name
        dest_file_name = reconstruct_dest_file_name(dest_file_name, str(suffix))
        path_for_decrypted_file = Path(path_for_decrypted_file.parent, dest_file_name)
        logger.info(f"New path_for_decrypted_file: {path_for_decrypted_file}")

        suffix += 1

    with open(file_to_decrypt, "rb") as f:
        key = crypt.key_from_file(f, key)

        with utils.get_progress_bar() as progress:
            progress.add_task("[cyan]Decrypting file...", total=None)
            crypt.decrypt(f, key, filename=path_for_decrypted_file)

    if not path_for_decrypted_file.exists():
        logger.error(f"Error: Decrypted file not found: {path_for_decrypted_file}")
        sys.exit(1)

    return path_for_decrypted_file


def log_decryption(
    config_file: Path, source_path, destination_path: Path, process_time: float | None
) -> None:
    """
    Logs the decryption of a file.

    Args:
        config_file (Path): The path to the configuration file.
        source_path (str): The path to the file before decryption.
        destination_path (str): The path to the file after decryption.
        process_time (float): The time it took to decrypt the file.

    Returns:
        None
    """

    decrypted_file = DecryptedFile(
        source_path=source_path,
        destination_path=destination_path,
        process_time=process_time,
    )

    query = decrypted_file.to_sql()

    db.execute_queries(config_file=config_file, queries=[query])
