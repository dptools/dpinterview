#!/usr/bin/env python
"""
Reconstructs a PHOENIX directory for pipeline.
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
from typing import List, Optional, Tuple
import shutil

from rich.logging import RichHandler

from pipeline import data, orchestrator
from pipeline.helpers import db, dpdash, utils, cli
from pipeline.helpers.config import config
from pipeline.helpers.timer import Timer
from pipeline.models.pulled_files import PulledFile

MODULE_NAME = "fake_root_gen"
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


def get_file_to_pull(
    config_file: Path, study_id: str
) -> Optional[Tuple[str, str, str]]:
    """
    Retrieves a file to decrypt from the database.

    Args:
        config_file (Path): The path to the config file.

    Returns:
        Optional[Tuple[str, str, str]]: A tuple containing the path to the file to decrypt,
            the interview type, and the interview name.
    """

    query = f"""
        SELECT interview_file, interview_type, interview_name
        FROM (
            SELECT interview_file, interview_type, interview_name,
                ROW_NUMBER() OVER (PARTITION BY md5 ORDER BY interview_name) AS row_number
            FROM (
                SELECT interview_file, interview_type, interview_name, md5
                FROM interview_files
                INNER JOIN interviews ON interview_files.interview_path = interviews.interview_path
                INNER JOIN files ON interview_files.interview_file = files.file_path
                WHERE interviews.study_id = '{study_id}'
                AND interview_files.interview_file_tags LIKE '%%video%%'
                AND md5 NOT IN (
                    SELECT md5 FROM decrypted_files
                    INNER JOIN files ON decrypted_files.source_path = files.file_path
                )
            ) AS all_files
        ) AS ranked_files
        WHERE row_number = 1
        LIMIT 1;
        """

    df = db.execute_sql(config_file=config_file, query=query)

    if df.empty:
        return None

    file_to_decrypt = df["interview_file"].iloc[0]
    interview_type = df["interview_type"].iloc[0]
    interview_name = df["interview_name"].iloc[0]

    return file_to_decrypt, interview_type, interview_name


def construct_dest_dir(
    config_file: Path,
    encrypted_file_path: Path,
    interview_type: str,
    study_id: str,
    data_root: Path,
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

    fake_root_dir = orchestrator.translate_to_fake_root(
        config_file=config_file, file_path=destination_dir
    )

    if not fake_root_dir.exists():
        fake_root_dir.mkdir(parents=True)

    return Path(fake_root_dir)


def construct_dest_file_name(
    file_to_decrypt: Path, interview_name: str, interview_type: str
) -> str:
    """
    Constructs a dpdash compliant  destination file name for the decrypted file.

    Args:
        file_to_decrypt (str): The path to the file to decrypt.
        interview_name (str): The name of the interview.
    """
    ext = file_to_decrypt.suffixes[-1]
    dp_dash_dict = dpdash.parse_dpdash_name(interview_name)
    dp_dash_dict["category"] = "audioVideo"
    dp_dash_dict["optional_tag"] = [interview_type]

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


def pull_file(
    config_file: Path, file_to_pull: Path, path_for_pulled_file: Path
) -> Path:
    """
    Pulls a file from the data directory to the fake root directory.

    Args:
        config_file (str): The path to the configuration file containing the key.
        file_to_pull (str): The path to the file to pull.
        path_for_pulled_file (str): The path to save the pulled file.

    Returns:
        None
    """
    data_root = orchestrator.get_data_root(config_file=config_file)

    suffix: int = 1
    while path_for_pulled_file.exists():
        logger.warning(f"Error: File already exists: {path_for_pulled_file}")
        logger.warning(f"Appending suffix: {suffix}")
        dest_file_name = path_for_pulled_file.name
        dest_file_name = reconstruct_dest_file_name(dest_file_name, str(suffix))
        path_for_pulled_file = Path(path_for_pulled_file.parent, dest_file_name)
        logger.info(f"New path_for_pulled_file: {path_for_pulled_file}")

        suffix += 1

    with utils.get_progress_bar() as progress:
        logger.debug(f"Copying file: {file_to_pull} -> {path_for_pulled_file}")
        progress.add_task("Copying file...", total=None)
        shutil.copy(file_to_pull, path_for_pulled_file)

    if not path_for_pulled_file.exists():
        logger.error(f"Error: Pulled file not found: {path_for_pulled_file}")
        sys.exit(1)

    orchestrator.fix_permissions(config_file=config_file, file_path=data_root)

    return path_for_pulled_file


def log_pull(
    config_file: Path, source_path, destination_path: Path, process_time: float | None
) -> None:
    """
    Logs the pull to the database.

    Args:
        config_file (Path): The path to the configuration file.
        source_path (str): The path to the file before decryption.
        destination_path (str): The path to the file after decryption.
        process_time (float): The time it took to decrypt the file.

    Returns:
        None
    """

    pulled_file = PulledFile(
        source_path=source_path,
        destination_path=destination_path,
        process_time=process_time,
    )

    query = pulled_file.to_sql()

    db.execute_queries(config_file=config_file, queries=[query])


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="decryption", description="Module to decrypt files."
    )
    parser.add_argument(
        "-c", "--config", type=str, help="Path to the config file.", required=False
    )
    parser.add_argument(
        "-n",
        "--num_files_to_decrypt",
        type=int,
        help="Number of files to decrypt.",
        required=False,
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

    config_params = config(config_file, section="general")
    studies = orchestrator.get_studies(config_file=config_file)
    data_root = orchestrator.get_data_root(config_file=config_file, enforce_real=True)

    # Get decryption count from command line if specified
    if args.num_files_to_decrypt:
        decrytion_count = args.num_files_to_decrypt
    else:
        decrytion_count = orchestrator.get_decryption_count(config_file=config_file)

    COUNTER = 0

    while True:
        study_id = studies[0]
        logger.info(f"Processing study: {study_id}")

        if orchestrator.check_if_decryption_requested(config_file=config_file):
            # Update decryption_count
            decrytion_count = orchestrator.get_decryption_count(config_file=config_file)
            logger.info(f"decrytion_count: {decrytion_count}")

            while COUNTER < decrytion_count:
                file_to_decrypt_t = get_file_to_pull(
                    config_file=config_file, study_id=study_id
                )

                if not file_to_decrypt_t:
                    logger.info("No file to decrypt. Continuing...")

                    if study_id == studies[-1]:
                        # Log to database
                        data.log(
                            config_file=config_file,
                            module_name=MODULE_NAME,
                            message=f"Decrypted {COUNTER} files.",
                        )

                        sys.exit(0)
                    else:
                        study_id = studies[studies.index(study_id) + 1]
                        logger.info(f"Processing study: {study_id}")
                        continue

                file_to_decrypt_path = Path(file_to_decrypt_t[0])
                interview_type = file_to_decrypt_t[1]
                interview_name = file_to_decrypt_t[2]

                dest_dir = construct_dest_dir(
                    config_file=config_file,
                    encrypted_file_path=file_to_decrypt_path,
                    interview_type=interview_type,
                    study_id=study_id,
                    data_root=data_root,
                )

                dest_file_name = construct_dest_file_name(
                    file_to_decrypt=file_to_decrypt_path,
                    interview_name=interview_name,
                    interview_type=interview_type,
                )

                path_for_decrypted_file = Path(dest_dir, dest_file_name)

                logger.info(f"Decrypting file: {file_to_decrypt_path}")
                logger.info(f"Saving to: {path_for_decrypted_file}")

                # Decrypt file
                with Timer() as timer:
                    path_for_decrypted_file = pull_file(
                        config_file=config_file,
                        file_to_pull=file_to_decrypt_path,
                        path_for_pulled_file=path_for_decrypted_file,
                    )

                # Log decryption
                log_pull(
                    config_file=config_file,
                    source_path=file_to_decrypt_path,
                    destination_path=path_for_decrypted_file,
                    process_time=timer.duration,
                )

                COUNTER += 1

            logger.info(f"Decrypted {COUNTER} files.")
            # Log to database
            data.log(
                config_file=config_file,
                module_name=MODULE_NAME,
                message=f"Decrypted {COUNTER} files.",
            )

            logger.info("Decryption complete.")
            # Set key_store to disabled
            orchestrator.complete_decryption(config_file=config_file)

        else:
            # Snooze if decryption is not requested
            orchestrator.snooze(config_file=config_file)

            # Update decryption_count
            decrytion_count = orchestrator.get_decryption_count(config_file=config_file)

            # Reset counter
            COUNTER = 0
