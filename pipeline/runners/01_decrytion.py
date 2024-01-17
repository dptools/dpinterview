#!/usr/bin/env python

import sys
from pathlib import Path

file = Path(__file__).resolve()
parent = file.parent
root = None
for parent in file.parents:
    if parent.name == "av-pipeline-v2":
        root = parent
sys.path.append(str(root))

# remove current directory from path
try:
    sys.path.remove(str(parent))
except ValueError:
    pass

import logging
from typing import Optional, Tuple, List

import cryptease as crypt
from rich.logging import RichHandler

from pipeline import orchestrator, data
from pipeline.helpers import db, dpdash, utils
from pipeline.helpers.config import config
from pipeline.models.decrypted_files import DecryptedFile
from pipeline.helpers.timer import Timer

MODULE_NAME = "decryption"
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


def get_file_to_decrypt(config_file: Path) -> Optional[Tuple[str, str, str]]:
    config_params = config(config_file, section="general")
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
    PARTICIPANT_ID = str(encrypted_file_path).split("/")[-5]

    DEST_DIR = Path(
        data_root,
        "PROTECTED",
        study_id,
        PARTICIPANT_ID,
        f"{interview_type}_interview",
        "processed",
        "decrypted",
    )

    if not DEST_DIR.exists():
        DEST_DIR.mkdir(parents=True)

    return Path(DEST_DIR)


def construct_dest_file_name(file_to_decrypt: Path, interview_name: str) -> str:
    ext = file_to_decrypt.suffixes[-2]
    dp_dash_dict = dpdash.parse_dpdash_name(interview_name)
    dp_dash_dict["category"] = "audioVideo"

    dest_file_name = dpdash.get_dpdash_name_from_dict(dp_dash_dict)
    dest_file_name = f"{dest_file_name}{ext}"

    return dest_file_name


def reconstruct_dest_file_name(dest_file_name: str, suffix: str) -> str:
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
    params = config(config_file, section="decryption")
    key_file = params["key_file"]
    print(f"Using decryption key from: {key_file}")

    # Check if key_file exists
    if not Path(key_file).exists():
        print(f"Error: key_file '{key_file}' does not exist.")
        sys.exit(1)

    # Get key from key_file
    with open(key_file, "r") as f:
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

    db.execute_queries(config_file=config_file, queries=[query], logger=logger)


if __name__ == "__main__":
    config_file = utils.get_config_file_path()
    utils.configure_logging(
        config_file=config_file, module_name=MODULE_NAME, logger=logger
    )

    console.rule(f"[bold red]{MODULE_NAME}")
    logger.info(f"Using config file: {config_file}")

    config_params = config(config_file, section="general")
    study_id = config_params["study"]
    data_root = Path(config_params["data_root"])

    # Get decryption count from command line if specified
    if len(sys.argv) > 2:
        print(f"Usage: {sys.argv[0]} <num_files_to_decrypt>")
        sys.exit(1)
    elif len(sys.argv) == 2:
        decrytion_count = int(sys.argv[1])
    else:
        decrytion_count = orchestrator.get_decryption_count(config_file=config_file)

    counter = 0

    while True:
        if orchestrator.check_if_decryption_requested(config_file=config_file):
            # Update decryption_count
            decrytion_count = orchestrator.get_decryption_count(config_file=config_file)
            logger.info(f"decrytion_count: {decrytion_count}")

            while counter < decrytion_count:
                file_to_decrypt_t = get_file_to_decrypt(config_file=config_file)

                if not file_to_decrypt_t:
                    console.print("No file to decrypt. Exiting...")
                    sys.exit(0)

                file_to_decrypt_path = Path(file_to_decrypt_t[0])
                interview_type = file_to_decrypt_t[1]
                interview_name = file_to_decrypt_t[2]

                dest_dir = construct_dest_dir(
                    encrypted_file_path=file_to_decrypt_path,
                    interview_type=interview_type,
                    study_id=study_id,
                    data_root=data_root,
                )

                dest_file_name = construct_dest_file_name(
                    file_to_decrypt=file_to_decrypt_path,
                    interview_name=interview_name,
                )

                path_for_decrypted_file = Path(dest_dir, dest_file_name)

                logger.info(f"Decrypting file: {file_to_decrypt_path}")
                logger.info(f"Saving to: {path_for_decrypted_file}")

                # Decrypt file
                with Timer() as timer:
                    path_for_decrypted_file = decrypt_file(
                        config_file=config_file,
                        file_to_decrypt=file_to_decrypt_path,
                        path_for_decrypted_file=path_for_decrypted_file,
                    )

                # Log decryption
                log_decryption(
                    config_file=config_file,
                    source_path=file_to_decrypt_path,
                    destination_path=path_for_decrypted_file,
                    process_time=timer.duration,
                )

                counter += 1

            # Log to database
            data.log(
                config_file=config_file,
                module_name=MODULE_NAME,
                message=f"Decrypted {counter} files.",
            )

            # Set key_store to disabled
            orchestrator.complete_decryption(config_file=config_file)

        else:
            # Snooze if decryption is not requested
            orchestrator.snooze(config_file=config_file)

            # Update decryption_count
            decrytion_count = orchestrator.get_decryption_count(config_file=config_file)

            # Reset counter
            counter = 0
