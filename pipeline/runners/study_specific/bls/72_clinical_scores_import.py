#!/usr/bin/env python
"""
Export metrics from REDCap and OpenFace to the metrics table.
Specfic to BLS.
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
from typing import Any, Dict, List, Optional

import pandas as pd
from rich.logging import RichHandler

from pipeline import constants, core
from pipeline.helpers import cli, db, utils
from pipeline.models.interview_roles import InterviewRole
from pipeline.models.metrics import Metrics

demographics_cols = ["age", "race", "gender", "diagnosis", "education"]
clinical_cols = [
    "ymrs",
    "madrs",
    "mcas",
    "panss_positive",
    "panss_negative",
    "panss_general",
]

MODULE_NAME = "pipeline.runners.study_specific.bls.72_clinical_scores_import"
logger = logging.getLogger(MODULE_NAME)
logargs = {
    "level": logging.DEBUG,
    # "format": "%(asctime)s - %(process)d - %(name)s - %(levelname)s - %(message)s",
    "format": "%(message)s",
    "handlers": [RichHandler(rich_tracebacks=True)],
}
logging.basicConfig(**logargs)

console = utils.get_console()


def map_sex_gender(value: int) -> str:
    """
    Maps gender from int to string.

    Based on mapping from Data Dictionary

    Args:
        value (int): The value to map.

    Returns:
        str: The mapped value.

    Raises:
        ValueError: If the value is not in the mapping.
    """
    if pd.isnull(value):
        value = 1000
    value = int(value)
    result = None

    match value:
        case 0:
            result = "Male"
        case 1:
            result = "Female"
        case _:
            raise ValueError

    return str(result)


def map_handedness(value: int) -> str:
    """
    Maps handedness from int to string.

    Args:
        value (int): The value to map.

    Returns:
        str: The mapped value.
    """
    if pd.isnull(value):
        value = 1000
    value = int(value)
    result = None

    match value:
        case 0:
            result = "Right Handed"
        case 1:
            result = "Left Handed"
        case _:
            result = "Prefer not to answer / Unknown"

    return str(result)


def map_edu(value: int) -> str:
    """
    Maps Education level based on mapping from Data Dictionary.

    Args:
        value (int): The value to map.

    Returns:
        str: The mapped value.
    """
    if pd.isnull(value):
        value = 1000
    value = int(value)
    result = None

    match value:
        case 1:
            result = "1. Grade 6 or less"
        case 2:
            result = "2. Grade 7 to 12 (without graduating high school)"
        case 3:
            result = "3. Graduated high school or high school equivalent"
        case 4:
            result = "4. Part college"
        case 5:
            result = "5. Graduated 2 year college"
        case 6:
            result = "6. Graduated 4 year college"
        case 7:
            result = "7. Part graduate/professional school"
        case 8:
            result = "8. Completed graduate/professional school"
        case 1000:
            result = "Prefer not to answer / Unknown"

    return str(result)


def map_race(value: int) -> str:
    """
    Maps race from int to string, based on mapping from Data Dictionary.

    Args:
        value (int): The value to map.

    Returns:
        str: The mapped value.
    """
    # 1, White | 2, Asian | 3, African American |
    # 4, American Indian/Hawaiian/Pacific Islander | 5, Other | 6, Prefer not to answer
    if pd.isnull(value):
        value = 1000
    value = int(value)
    result = None

    match value:
        case 1:
            result = "White"
        case 2:
            result = "Asian"
        case 3:
            result = "African American"
        case 4:
            result = "American Indian/Hawaiian/Pacific Islander"
        case 5:
            result = "Other"
        case _:
            result = "Prefer not to answer"

    return str(result)


def get_demographics(
    study_id: str, subject_id: str, demographics: List[str], data_root: Path
) -> Optional[Dict[str, Any]]:
    """
    Get the demographics for a subject from raw REDCap data (CSVs).

    Args:
        study_id (str): The study ID.
        subject_id (str): The subject ID.
        demographics (List[str]): The demographics to get.
        data_root (Path): The PHOENIX structured data root.

    Returns:
        Optional[Dict[str, Any]]: The demographics, or None if they could not be found.
    """
    redcap_path = data_root / "PROTECTED" / study_id / subject_id / "redcap" / "raw"
    demographics_csvs = list(redcap_path.glob("*redcap_demographic.csv"))
    if len(demographics_csvs) == 0:
        return None

    demographics_csv = demographics_csvs[0]
    demo_df = pd.read_csv(demographics_csv)

    demo_dict: Dict[str, Any] = {}
    for col in demographics:
        if col in demo_df.columns:
            demo_dict[col] = demo_df[col].values[0]

            if col == "race":
                demo_dict[col] = map_race(demo_dict[col])
            elif col == "gender":
                demo_dict[col] = map_sex_gender(demo_dict[col])
            elif col == "education":
                demo_dict[col] = map_edu(demo_dict[col])
        else:
            demo_dict[col] = None

    return demo_dict


def get_clinical_scores(
    study_id: str,
    subject_id: str,
    day: int,
    clinical_scores: List[str],
    data_root: Path,
) -> Optional[Dict[str, Any]]:
    """
    Get the clinical scores for a subject from processed REDCap data (CSVs).

    Args:
        study_id (str): The study ID.
        subject_id (str): The subject ID.
        day (int): The day to get the scores for.
        clinical_scores (List[str]): The clinical scores to get.
        data_root (Path): The PHOENIX structured data root.

    Returns:
        Optional[Dict[str, Any]]: The clinical scores, or None if they could not be found.
    """
    redcap_path = (
        data_root / "PROTECTED" / study_id / subject_id / "redcap" / "processed"
    )
    clinical_csvs = list(redcap_path.glob("*redcap_clinical_score*.csv"))
    if len(clinical_csvs) == 0:
        return None
    elif len(clinical_csvs) > 1:
        raise ValueError(f"Multiple clinical score files found for {subject_id}")

    clinical_csv = clinical_csvs[0]
    clinical_df = pd.read_csv(clinical_csv)

    adjusted_day = day - 1
    clinical_df = clinical_df[clinical_df["day"] == adjusted_day]
    if clinical_df.shape[0] == 0:
        return None

    clinical_dict: Dict[str, Any] = {}
    for col in clinical_scores:
        if col in clinical_df.columns:
            clinical_dict[col] = float(clinical_df[col].values[0])
        else:
            clinical_dict[col] = None

    return clinical_dict


def has_report(
    config_file: Path,
    interview_name: str,
) -> bool:
    """
    Check if a report exists in the database.

    Args:
        config_file (Path): The path to the config file.
        interview_name (str): The interview name.

    Returns:
        bool: True if the report exists, False otherwise.
    """
    query = f"""
    SELECT * FROM pdf_reports
    WHERE interview_name = '{interview_name}';
    """
    df = db.execute_sql(config_file=config_file, query=query)
    return not df.empty


def get_openface_metrics(
    config_file: Path, subject_id: str, study_id: str, interview_name: str
) -> Dict[str, Any]:
    """
    Get the OpenFace metrics for a subject. This includes pose and action unit mean,
    std, and correlation.

    Args:
        config_file (Path): The path to the config file.
        subject_id (str): The subject ID.
        study_id (str): The study ID.
        interview_name (str): The interview name.

    Returns:
        Dict[str, Any]: The OpenFace metrics.
    """

    of_data = core.fetch_openface_features(
        config_file=config_file,
        interview_name=interview_name,
        subject_id=subject_id,
        study_id=study_id,
        role=InterviewRole.SUBJECT,
        cols=constants.POSE_COLS + constants.AU_COLS + ["timestamp"],
    )

    of_metrics = core.construct_openface_metrics(session_openface_features=of_data)

    qc_df = core.fetch_openface_qc(
        interview_name=interview_name,
        ir_role=InterviewRole.SUBJECT,
        config_file=config_file,
    )

    qc = qc_df.to_dict(orient="records")[0]
    of_metrics["qc"] = qc

    return of_metrics


def get_metrics(
    interview_name: str,
    data_root: Path,
    config_file: Path,
) -> Optional[Metrics]:
    """
    Get the metrics for an interview.

    Args:
        interview_name (str): The interview name.
        data_root (Path): The PHOENIX structured data root.
        config_file (Path): The path to the config file.

    Returns:
        Metrics: The metrics.
    """
    query = f"""
    SELECT subject_id, study_id, lof_notes, lof_report_generation_possible FROM load_openface
    WHERE interview_name = '{interview_name}';
    """

    df = db.execute_sql(config_file=config_file, query=query)

    if df.empty:
        return None

    row = df.iloc[0]
    subject_id = row["subject_id"]
    study_id = row["study_id"]
    notes = row["lof_notes"]
    report_generation_queued = bool(row["lof_report_generation_possible"])
    interview_has_report = has_report(config_file, interview_name)

    if interview_has_report:
        interview_status = "Report"
    elif report_generation_queued:
        interview_status = "Queued"
    else:
        interview_status = notes

    # Get interview day from interview name
    # sample interview name: "BLS-ZXD8M-offsiteInterview-day0443"

    interview_day = int(interview_name.split("-")[-1].replace("day", ""))

    demo_dict = get_demographics(
        study_id=study_id,
        subject_id=subject_id,
        demographics=demographics_cols,
        data_root=data_root,
    )

    clinical_dict = get_clinical_scores(
        study_id=study_id,
        subject_id=subject_id,
        day=interview_day,
        clinical_scores=clinical_cols,
        data_root=data_root,
    )

    if demo_dict is None:
        demo_dict = {col: None for col in demographics_cols}
    if clinical_dict is None:
        clinical_dict = {col: None for col in clinical_cols}

    try:
        of_metrics = get_openface_metrics(
            config_file=config_file,
            subject_id=subject_id,
            study_id=study_id,
            interview_name=interview_name,
        )
    except ValueError as e:
        logger.error(f"Error getting OpenFace metrics for {interview_name}: {e}")
        of_metrics = None
        interview_status = f"{interview_status} | Error: {e}"

    metrics = {
        "subject_id": subject_id,
        "study_id": study_id,
        "interview_name": interview_name,
        "interview_status": interview_status,
        "day": interview_day,
        "demographics": demo_dict,
        "clinical_scores": clinical_dict,
        "openface_metrics": of_metrics,
    }

    return Metrics(interview_name=interview_name, metrics=metrics)


def populate_metrics(
    config_file: Path,
) -> None:
    """
    Populate the metrics table with metrics specific to BLS.

    Args:
        config_file (Path): The path to the config file.

    Returns:
        None
    """

    general_params = utils.config(path=config_file, section="general")
    data_root = Path(general_params["data_root"])

    query = """
    SELECT interview_name FROM load_openface
    WHERE study_id = 'BLS' AND
        interview_name NOT IN (
            SELECT interview_name FROM metrics
        )
    ORDER BY RANDOM();
    """

    df = db.execute_sql(config_file=config_file, query=query)
    logger.info(f"Found {df.shape[0]} interviews to process.")

    with utils.get_progress_bar() as progress:
        task = progress.add_task("Processing interviews", total=df.shape[0])

        for _, row in df.iterrows():
            progress.update(task, description=row["interview_name"], advance=1)
            interview_name = str(row["interview_name"])
            metrics = get_metrics(
                interview_name=interview_name,
                data_root=data_root,
                config_file=config_file,
            )

            if metrics is not None:
                sql_query = [metrics.to_sql()]
                db.execute_queries(
                    config_file=config_file,
                    queries=sql_query,
                    show_commands=False,
                    silent=True,
                )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="dropbox sync", description="Module to sync PDF reports to Dropbox."
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

    populate_metrics(config_file=config_file)

    logger.info("Done.")
