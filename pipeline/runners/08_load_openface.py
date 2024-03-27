#!/usr/bin/env python
"""
Loads OpenFace Features into openface_db

Determines in Interview is ready for report generation.
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

from pipeline import orchestrator
from pipeline.core import load_openface
from pipeline.helpers import cli, utils

MODULE_NAME = "load_openface"

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
        prog="decryption", description="Module to decrypt files."
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

    logger.info("[bold green]Starting load_openface loop...", extra={"markup": True})
    study_id = studies[0]
    logger.info(f"Statring with study: {study_id}")

    while True:
        interview_name = load_openface.get_interview_to_process(
            config_file=config_file, study_id=study_id
        )

        if interview_name is None:
            if study_id == studies[-1]:
                # Log if any files were processed
                if COUNTER > 0:
                    orchestrator.log(
                        config_file=config_file,
                        module_name=MODULE_NAME,
                        message=f"Loaded OpenFace features for {COUNTER} interviews.",
                    )
                    COUNTER = 0

                # Snooze if no files to process
                orchestrator.snooze(config_file=config_file)
                study_id = studies[0]
                continue
            else:
                study_id = studies[studies.index(study_id) + 1]
                logger.info(f"Switching to study: {study_id}")
                continue

        COUNTER += 1

        logger.info(
            f"[cyan]Loading OpenFace features for {interview_name}...",
            extra={"markup": True},
        )

        of_runs = load_openface.get_openface_runs(
            config_file=config_file, interview_name=interview_name
        )

        lof = load_openface.construct_load_openface(
            interview_name=interview_name, of_runs=of_runs, config_file=config_file
        )
        lof = load_openface.import_of_openface_db(config_file=config_file, lof=lof)

        load_openface.log_load_openface(config_file=config_file, lof=lof)
