#!/usr/bin/env python
"""
Generate PDF reports for the pipeline.
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
from datetime import datetime
from typing import List

from rich.logging import RichHandler

from pipeline import healer, orchestrator
from pipeline.core import report
from pipeline.helpers import cli, db, utils
from pipeline.helpers.timer import Timer
from pipeline.models.pdf_reports import PdfReport
from pipeline.models.exported_assets import ExportedAsset

MODULE_NAME = "report_regeneration"

logger = logging.getLogger(MODULE_NAME)
logargs = {
    "level": logging.DEBUG,
    # "format": "%(asctime)s - %(process)d - %(name)s - %(levelname)s - %(message)s",
    "format": "%(message)s",
    "handlers": [RichHandler(rich_tracebacks=True)],
}
logging.basicConfig(**logargs)

console = utils.get_console()

# Silence logs from other modules
noisy_modules: List[str] = [
    "PIL.PngImagePlugin",
    "svglib.svglib",
    "matplotlib.font_manager",
]
for module in noisy_modules:
    logger.debug(f"Setting log level for {module} to INFO")
    logging.getLogger(module).setLevel(logging.INFO)


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

    config_params = utils.config(config_file, section="general")
    studies = orchestrator.get_studies(config_file=config_file)

    COUNTER = 0

    logger.info(
        "[bold green]Starting report_generation loop...", extra={"markup": True}
    )
    study_id = studies[0]
    logger.info(f"Starting with study: {study_id}")

    report_params = utils.config(config_file, section="report_generation")
    report_version = report_params["report_version"]

    while True:
        # Get interview name to process
        interview_name = report.get_interview_name_to_process(
            config_file=config_file, study_id=study_id, report_version=report_version
        )

        if interview_name is None:
            if study_id == studies[-1]:
                # Log if any reports were generated
                if COUNTER > 0:
                    orchestrator.log(
                        config_file=config_file,
                        module_name=MODULE_NAME,
                        message=f"Generated {COUNTER} reports.",
                    )
                    COUNTER = 0

                # Snooze if no interviews to process
                orchestrator.snooze(config_file=config_file)
                study_id = studies[0]
                logger.info(f"Restarting with study: {study_id}")
                continue
            else:
                study_id = studies[studies.index(study_id) + 1]
                logger.info(f"Switching to study: {study_id}")
                continue

        COUNTER += 1
        logger.info(
            f"[cyan]Generating report for {interview_name}...",
            extra={"markup": True},
        )

        report_path = report.construct_report_path(
            config_file=config_file, interview_name=interview_name
        )

        with Timer() as timer:
            ERROR_MESSAGE = report.generate_report(
                config_file=config_file,
                interview_name=interview_name,
                report_path=report_path,
            )

        pr_generation_time = timer.duration
        logger.info(f"Generated report in {pr_generation_time:.2f} seconds")

        if ERROR_MESSAGE:
            logger.warning(
                f"Error generating report for {interview_name}: {ERROR_MESSAGE}"
            )
            healer.set_report_generation_not_possible(
                config_file=config_file,
                interview_name=interview_name,
                reason=ERROR_MESSAGE,
            )
            continue

        pdf_report = PdfReport(
            interview_name=interview_name,
            pr_version=report_version,
            pr_path=str(report_path),
            pr_generation_time=pr_generation_time,
            pr_timestamp=datetime.now(),
        )

        # Place in GENERAL directory
        data_root = orchestrator.get_data_root(config_file=config_file)
        real_data_root = orchestrator.get_data_root(
            config_file=config_file, enforce_real=True
        )

        general_path = Path(
            str(report_path).replace(str(data_root), str(real_data_root))
        )

        asset: ExportedAsset = ExportedAsset(
            interview_name=interview_name,
            asset_path=report_path,
            asset_type="file",
            asset_export_type="GENERAL",
            asset_tag="anonymized_pdf_report",
            asset_destination=general_path,
            aset_exported_timestamp=datetime.now(),
        )

        logger.info(f"Copying report from {report_path} to {general_path}")
        general_path.parent.mkdir(parents=True, exist_ok=True)
        cli.copy(report_path, general_path)

        query = asset.to_sql()
        db.execute_queries(config_file=config_file, queries=[query])
        report.log_pdf_report(config_file=config_file, pdf_report=pdf_report)
