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

from pipeline import orchestrator
from pipeline.core import transcript_quick_qc
from pipeline.helpers import cli, utils
from pipeline.helpers.timer import Timer

MODULE_NAME = "pipeline.runners.study_specific.bls.21_transcript_quick_qc"

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
        prog="transcript_quick_qc",
        description="Performs quality control on transcripts.",
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

    logger.info(
        "[bold green]Starting transcript_quick_qc loop...", extra={"markup": True}
    )
    study_id = studies[0]
    logger.info(f"Statring with study: {study_id}")

    while True:
        # Get file to process
        file_to_process = transcript_quick_qc.get_transcript_to_process(
            config_file=config_file, study_id=study_id
        )

        if file_to_process is None:
            if study_id == studies[-1]:
                # Log if any files were processed
                if COUNTER > 0:
                    orchestrator.log(
                        config_file=config_file,
                        module_name=MODULE_NAME,
                        message=f"Ran transcript_quick_qc on {COUNTER} files.",
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
            f"[cyan]Running Transcription Quick QC on {file_to_process.stem}...",
            extra={"markup": True},
        )

        with Timer() as timer:
            transcript_df = transcript_quick_qc.transcript_to_df(
                transcript_path=file_to_process
            )

            turn_data = transcript_quick_qc.get_turn_data(
                transcript_df=transcript_df
            )

            transcript_qqc = transcript_quick_qc.get_transcription_quick_qc(
                transcript_df=transcript_df
            )
            transcript_qqc = transcript_quick_qc.add_speaker_roles_to_qqc(
                qqc=transcript_qqc
            )

        process_time = timer.duration

        logger.info(
            f"[cyan]Logging Transcription Quick QC results for {file_to_process.stem}...",
            extra={"markup": True},
        )
        transcript_quick_qc.log_transcript_quick_qc(
            config_file=config_file,
            transcript_path=file_to_process,
            qqc=transcript_qqc,
            turn_data=turn_data,
            process_time=process_time,
        )
