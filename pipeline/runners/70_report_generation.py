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
from datetime import datetime
from typing import List, Optional

from rich.logging import RichHandler

from pipeline import data, orchestrator
from pipeline.helpers import cli, db, dpdash, utils
from pipeline.helpers.timer import Timer
from pipeline.models.pdf_reports import PdfReport
from pipeline.report import main as report

MODULE_NAME = "report_generation"

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


def get_interview_name_to_process(config_file: Path, study_id: str) -> Optional[str]:
    """
    Get the interview name to process from the database.

    Args:
        config_file (Path): The path to the config file.
        study_id (str): The study_id.

    Returns:
        Optional[str]: The interview name to process.
    """

    query = f"""
        SELECT interview_name
        FROM load_openface
        WHERE study_id = '{study_id}' AND
            lof_report_generation_possible = TRUE AND
            interview_name NOT IN (
                SELECT interview_name
                FROM pdf_reports
            )
        ORDER BY RANDOM()
        LIMIT 1;
    """

    interview_name = db.fetch_record(config_file=config_file, query=query)

    return interview_name


def construct_report_path(config_file: Path, interview_name: str) -> Path:
    """
    Construct the path to the report.

    Args:
        config_file (Path): The path to the config file.
        interview_name (str): The interview name.

    Returns:
        Path: The path to the report.
    """
    config_params = utils.config(path=config_file, section="general")
    data_root = Path(config_params["data_root"])

    dpdash_dict = dpdash.parse_dpdash_name(interview_name)

    study_id: str = dpdash_dict["study"]  # type: ignore
    subject_id: str = dpdash_dict["subject"]  # type: ignore

    interview_type = data.get_interview_type(
        interview_name=interview_name, config_file=config_file
    )

    reports_dir_path = (
        data_root
        / "PROTECTED"
        / study_id
        / subject_id
        / f"{interview_type}_interview"
        / "processed"
        / "reports"
    )

    if not reports_dir_path.exists():
        reports_dir_path.mkdir(parents=True, exist_ok=True)

    dpdash_dict["category"] = "report"
    report_name = dpdash.get_dpdash_name_from_dict(dpdash_dict)

    report_path = reports_dir_path / f"{report_name}.pdf"

    return report_path


def generate_report(config_file: Path, interview_name: str, report_path: Path) -> None:
    """
    Generate the report.

    Args:
        config_file (Path): The path to the config file.
        interview_name (str): The interview name.
        report_path (Path): The path to the report.
    """
    logger.info(f"Generating report for {interview_name}...")

    report.generate_report(
        config_file=config_file,
        interview_name=interview_name,
        dest_file_name=report_path,
    )

    return


def log_pdf_report(config_file: Path, pdf_report: PdfReport) -> None:
    """
    Logs the PDF report to the database.

    Args:
        config_file (Path): Path to the config file.
        pdf_report (PdfReport): Object containing the results of the report generation.
    """
    query = pdf_report.to_sql()

    db.execute_queries(config_file=config_file, queries=[query], show_commands=True)


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
    study_id = config_params["study"]

    COUNTER = 0

    logger.info(
        "[bold green]Starting report_generation loop...", extra={"markup": True}
    )

    while True:
        # Get interview name to process
        interview_name = get_interview_name_to_process(
            config_file=config_file, study_id=study_id
        )

        if interview_name is None:
            # Log if any reports were generated
            if COUNTER > 0:
                data.log(
                    config_file=config_file,
                    module_name=MODULE_NAME,
                    message=f"Generated {COUNTER} reports.",
                )
                COUNTER = 0

            # Snooze if no interviews to process
            orchestrator.snooze(config_file=config_file)
            continue

        COUNTER += 1
        logger.info(
            f"[cyan]Generating report for {interview_name}...",
            extra={"markup": True},
        )

        report_path = construct_report_path(
            config_file=config_file, interview_name=interview_name
        )

        with Timer() as timer:
            generate_report(
                config_file=config_file,
                interview_name=interview_name,
                report_path=report_path,
            )

        pr_generation_time = timer.duration
        logger.info(f"Generated report in {pr_generation_time:.2f} seconds")

        pdf_report = PdfReport(
            interview_name=interview_name,
            pr_version="v1.0.0",
            pr_path=str(report_path),
            pr_generation_time=pr_generation_time,
            pr_timestamp=datetime.now(),
        )

        log_pdf_report(config_file=config_file, pdf_report=pdf_report)
