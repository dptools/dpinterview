#!/usr/bin/env python
"""
Decryption Service

Note:
This module is study agnostic.
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

from rich.logging import RichHandler

from pipeline import orchestrator
from pipeline.core import decryption
from pipeline.helpers import cli, utils
from pipeline.helpers.timer import Timer
from pipeline.models.decrypted_files import DecryptedFile

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

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="decryption", description="Decrypt files requested for decryption."
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
    data_root = orchestrator.get_data_root(config_file=config_file)

    COUNTER = 0

    while True:
        files_to_decrypt = DecryptedFile.get_files_pending_decrytion(
            config_file=config_file
        )

        if files_to_decrypt.empty:
            logger.info("No files to decrypt.")
            orchestrator.snooze(config_file=config_file)

        for index, row in files_to_decrypt.iterrows():
            source_path = Path(row["source_path"])
            destination_path = Path(row["destination_path"])

            if destination_path.exists():
                logger.error(
                    f"Error: Decrypted file already exists: {destination_path}"
                )
                sys.exit(1)

            if not source_path.exists():
                logger.error(f"Error: File to decrypt does not exist: {source_path}")
                sys.exit(1)

            with Timer() as timer:
                logger.info(f"Decrypting file: {source_path} -> {destination_path}")
                decryption.decrypt_file(
                    config_file=config_file,
                    file_to_decrypt=source_path,
                    path_for_decrypted_file=destination_path,
                )
                orchestrator.fix_permissions(config_file=config_file, file_path=destination_path)

            DecryptedFile.update_decrypted_status(
                config_file=config_file,
                file_path=source_path,
                process_time=timer.duration,
            )
            COUNTER += 1

        if COUNTER >= 10:
            orchestrator.log(
                config_file=config_file,
                module_name=MODULE_NAME,
                message=f"Decrypted {COUNTER} files.",
            )
            COUNTER = 0
