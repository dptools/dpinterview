#!/usr/bin/env python
"""
Gather metadata for files
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
from pipeline.core import metadata
from pipeline.helpers import cli, ffprobe, utils

MODULE_NAME = "metadata"

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
        prog=MODULE_NAME, description="Gather metadata for files."
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

    orchestrator.redirect_temp_dir(config_file=config_file)

    config_params = utils.config(config_file, section="general")
    studies = orchestrator.get_studies(config_file=config_file)

    COUNTER = 0

    logger.info(
        "[bold green]Starting metadata gathering loop...", extra={"markup": True}
    )

    study_id = studies[0]
    logger.info(f"Starting with study: {study_id}", extra={"markup": True})
    while True:
        # Get file to process
        file_to_process = metadata.get_file_to_process(
            config_file=config_file, study_id=study_id
        )

        if file_to_process is None:
            if study_id == studies[-1]:
                # Log if any files were processed
                if COUNTER > 0:
                    orchestrator.log(
                        config_file=config_file,
                        module_name=MODULE_NAME,
                        message=f"Gathered metadata for {COUNTER} files.",
                    )
                    COUNTER = 0

                # Snooze if no files to process
                orchestrator.snooze(config_file=config_file)
                study_id = studies[0]
                logger.info(
                    f"Restarting with study: {study_id}", extra={"markup": True}
                )
                continue
            else:
                study_id = studies[studies.index(study_id) + 1]
                logger.info(f"Switching to study: {study_id}", extra={"markup": True})
                continue

        COUNTER += 1
        logger.info(
            f"[cyan]Getting Metadata for{file_to_process}...", extra={"markup": True}
        )

        with utils.get_progress_bar() as progress:
            task = progress.add_task("Fetching Metadafa using 'ffprobe'", total=None)
            metadata_dict = ffprobe.get_metadata(
                file_path_to_process=Path(file_to_process), config_file=config_file
            )

        metadata.log_metadata(
            source=Path(file_to_process),
            metadata=metadata_dict,
            config_file=config_file,
            requested_by=MODULE_NAME
        )
