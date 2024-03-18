#!/usr/bin/env python
"""
Decryption Runner
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

from rich.logging import RichHandler

from pipeline import core, orchestrator
from pipeline.core import fetch_video
from pipeline.helpers import cli, utils

MODULE_NAME = "fetch_video"
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
        prog="fetch_video", description="Fetch video files for decryption."
    )
    parser.add_argument(
        "-c", "--config", type=str, help="Path to the config file.", required=False
    )
    parser.add_argument(
        "-n",
        "--num_files_to_request",
        type=int,
        help="Number of files to request for decryption.",
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

    config_params = utils.config(config_file, section="general")
    studies = orchestrator.get_studies(config_file=config_file)
    data_root = orchestrator.get_data_root(config_file=config_file)

    # Get decryption count from command line if specified
    if args.num_files_to_request:
        decrytion_count = args.num_files_to_request
    else:
        decrytion_count = orchestrator.get_decryption_count(config_file=config_file)

    COUNTER = 0

    study_id = studies[0]
    logger.info(f"Starting with study: {study_id}", extra={"markup": True})
    while True:
        if orchestrator.check_if_decryption_requested(config_file=config_file):
            # Update decryption_count
            decrytion_count = orchestrator.get_decryption_count(config_file=config_file)
            logger.info(f"decrytion_count: {decrytion_count}")

            while COUNTER < decrytion_count:
                file_to_decrypt_t = fetch_video.get_file_to_decrypt(
                    config_file=config_file
                )

                if not file_to_decrypt_t:
                    if study_id == studies[-1]:
                        console.print("No file to decrypt. Exiting...")
                        sys.exit(0)
                    else:
                        study_id = studies[studies.index(study_id) + 1]
                        logger.info(
                            f"Switching to study: {study_id}", extra={"markup": True}
                        )
                        continue

                file_to_decrypt_path = Path(file_to_decrypt_t[0])
                interview_type = file_to_decrypt_t[1]
                interview_name = file_to_decrypt_t[2]

                dest_dir = fetch_video.construct_dest_dir(
                    encrypted_file_path=file_to_decrypt_path,
                    interview_type=interview_type,
                    study_id=study_id,
                    data_root=data_root,
                )

                dest_file_name = fetch_video.construct_dest_file_name(
                    file_to_decrypt=file_to_decrypt_path,
                    interview_name=interview_name,
                )

                path_for_decrypted_file = Path(dest_dir, dest_file_name)

                # Log decryption request
                fetch_video.log_decryption_request(
                    config_file=config_file,
                    source_path=file_to_decrypt_path,
                    destination_path=path_for_decrypted_file,
                )

                COUNTER += 1

            # Log to database
            core.log(
                config_file=config_file,
                module_name=MODULE_NAME,
                message=f"Requested decryption of {COUNTER} files.",
            )

            # Set key_store to disabled
            orchestrator.complete_decryption(config_file=config_file)

        else:
            # Snooze if decryption is not requested
            orchestrator.snooze(config_file=config_file)

            # Update decryption_count
            decrytion_count = orchestrator.get_decryption_count(config_file=config_file)

            # Reset counter
            COUNTER = 0
