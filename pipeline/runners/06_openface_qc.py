#!/usr/bin/env python
"""
Performs quality control on OpenFace output.
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

from pipeline import core, healer, orchestrator
from pipeline.core import openface_qc
from pipeline.helpers import cli, utils
from pipeline.helpers.timer import Timer

MODULE_NAME = "openface_qc"

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
        prog="openface_qc", description="Performs quality control on OpenFace output."
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

    COUNTER = 0

    logger.info("[bold green]Starting openface_qc loop...", extra={"markup": True})
    study_id = studies[0]
    logger.info(f"Statring with study: {study_id}")

    while True:
        # Get file to process
        file_to_process = openface_qc.get_file_to_process(
            config_file=config_file, study_id=study_id
        )

        if file_to_process is None:
            if study_id == studies[-1]:
                # Log if any files were processed
                if COUNTER > 0:
                    core.log(
                        config_file=config_file,
                        module_name=MODULE_NAME,
                        message=f"Ran OpenFace QC on {COUNTER} files.",
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
            f"[cyan]Running OpenFace QC on {file_to_process.stem}...",
            extra={"markup": True},
        )

        try:
            with Timer() as timer:
                openface_qc_result = openface_qc.run_openface_qc(
                    of_processed_path=file_to_process
                )

            openface_qc_result.ofqc_process_time = timer.duration
            openface_qc.log_openface_qc(
                config_file=config_file, openface_qc_result=openface_qc_result
            )
        except FileNotFoundError as e:
            logger.error(e)
            healer.clean_openface(config_file=config_file, openface_dir=file_to_process)
