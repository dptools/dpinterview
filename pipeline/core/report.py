"""
Helper functions for the report generation module.
"""

import logging
from pathlib import Path
from typing import Optional

import matplotlib.pyplot as plt

from pipeline import core, orchestrator
from pipeline.helpers import db, dpdash, utils
from pipeline.models.pdf_reports import PdfReport
from pipeline.report import main as report

logger = logging.getLogger(__name__)


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


def is_anonimization_requested(config_file: Path) -> bool:
    """
    Check if the anonymization is requested from the config file.

    Args:
        config_file (Path): The path to the config file.

    Returns:
        bool: True if anonymization is requested, False otherwise.
    """

    report_params = utils.config(config_file, section="report_generation")
    anonymize_s = report_params["anonymize"].lower()

    # str to bool
    if anonymize_s == "true":
        anonymize = True
    else:
        anonymize = False

    return anonymize


def construct_report_path(config_file: Path, interview_name: str) -> Path:
    """
    Construct the path to the report.

    Args:
        config_file (Path): The path to the config file.
        interview_name (str): The interview name.

    Returns:
        Path: The path to the report.
    """
    data_root = orchestrator.get_data_root(config_file=config_file)

    dpdash_dict = dpdash.parse_dpdash_name(interview_name)

    study_id: str = dpdash_dict["study"]  # type: ignore
    subject_id: str = dpdash_dict["subject"]  # type: ignore

    interview_type = core.get_interview_type(
        interview_name=interview_name, config_file=config_file
    )

    if is_anonimization_requested(config_file=config_file):
        data_dir = "GENERAL"
    else:
        data_dir = "PROTECTED"

    reports_dir_path = (
        data_root
        / data_dir
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


def generate_report(
    config_file: Path, interview_name: str, report_path: Path
) -> Optional[str]:
    """
    Generate the report.

    Args:
        config_file (Path): The path to the config file.
        interview_name (str): The interview name.
        report_path (Path): The path to the report.

    Returns:
        Optional[str]: The error message, if any.
    """

    error_message = report.generate_report(
        config_file=config_file,
        interview_name=interview_name,
        dest_file_name=report_path,
    )
    plt.close()

    return error_message


def log_pdf_report(config_file: Path, pdf_report: PdfReport) -> None:
    """
    Logs the PDF report to the database.

    Args:
        config_file (Path): Path to the config file.
        pdf_report (PdfReport): Object containing the results of the report generation.
    """
    query = pdf_report.to_sql()

    db.execute_queries(config_file=config_file, queries=[query], show_commands=True)
