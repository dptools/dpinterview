#!/usr/bin/env python
"""
Finds Interviews from imported forms_data (run sheets)
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

import logging
import argparse
from typing import List
import datetime

from rich.logging import RichHandler

from pipeline import orchestrator
from pipeline.helpers import cli, utils, db, dpdash
from pipeline.models.expected_interviews import ExpectedInterview

MODULE_NAME = "import_expected_interviews"
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


def get_open_interviews(study: str, config_file: Path) -> List[ExpectedInterview]:
    """
    Retrieves the expected interviews based on form_data

    Args:
        study (str): The study name.
        config_file (Path): The path to the config file.

    Returns:
        List[ExpectedInterview]: A list of ExpectedInterview objects.
    """
    expected_interviews: List[ExpectedInterview] = []

    sql_query = f"""
    SELECT
        subject_id,
        study_id,
        form_name,
        event_name,
        form_data ->> 'chrspeech_interview_date' AS chrspeech_interview_date,
        consent_date
    FROM public.form_data
    LEFT JOIN subjects USING (subject_id, study_id)
    WHERE (
            form_data->>'chrspeech_missing' IS NULL
            OR form_data->>'chrspeech_missing' <> '1'
        )
        AND form_name = 'speech_sampling_run_sheet'
        AND form_data ->> 'chrspeech_upload' = '1'
        AND form_data ->> 'chrspeech_interview_date' IS NOT NULL
        AND study_id = '{study}'
    """

    result_df = db.execute_sql(
        config_file=config_file,
        query=sql_query,
    )

    for _, row in result_df.iterrows():
        subject_id = row["subject_id"]
        study_id = row["study_id"]
        form_name = row["form_name"]
        event_name = row["event_name"]

        chrspeech_interview_date = row["chrspeech_interview_date"]
        interview_date = datetime.datetime.strptime(
            chrspeech_interview_date, "%Y-%m-%d"
        )

        consent_date = row["consent_date"]

        # Convert consent_date to datetime.datetime for compatibility
        if isinstance(consent_date, str):
            consent_date = datetime.datetime.strptime(consent_date, "%Y-%m-%d")
        elif isinstance(consent_date, datetime.date):
            consent_date = datetime.datetime.combine(
                consent_date, datetime.datetime.min.time()
            )

        days = dpdash.get_days_between_dates(
            consent_date=consent_date, event_date=interview_date
        )

        interview_name = dpdash.get_dpdash_name(
            subject=subject_id,
            study=study_id,
            data_type="openInterview",
            consent_date=consent_date,
            event_date=interview_date,
        )

        expected_interview = ExpectedInterview(
            interview_name=interview_name,
            subject_id=subject_id,
            study_id=study_id,
            form_name=form_name,
            event_name=event_name,
            expected_interview_date=interview_date,
            expected_interview_day=days,
            expected_interview_type="open",
        )

        expected_interviews.append(expected_interview)

    return expected_interviews


def get_psychs_interviews(study: str, config_file: Path) -> List[ExpectedInterview]:
    """
    Retrieves the expected interviews based on form_data

    Args:
        study (str): The study name.
        config_file (Path): The path to the config file.

    Returns:
        List[ExpectedInterview]: A list of ExpectedInterview objects.
    """
    expected_interviews: List[ExpectedInterview] = []

    sql_query = f"""
    SELECT
        subject_id,
        study_id,
        form_name,
        event_name,
        form_data ->> 'chrpsychs_av_interview_date' AS chrpsychs_av_interview_date,
        consent_date
    FROM public.form_data
    LEFT JOIN subjects USING (subject_id, study_id)
    WHERE (
            form_data->>'chrpsychs_av_missing' IS NULL
            OR form_data->>'chrpsychs_av_missing' <> '1'
        )
        AND form_name = 'psychs_av_recording_run_sheet'
        AND form_data ->> 'chrpsychs_av_upload' = '1'
        AND form_data ->> 'chrpsychs_av_interview_date' IS NOT NULL
        AND study_id = '{study}'
    """

    result_df = db.execute_sql(
        config_file=config_file,
        query=sql_query,
    )

    for _, row in result_df.iterrows():
        subject_id = row["subject_id"]
        study_id = row["study_id"]
        form_name = row["form_name"]
        event_name = row["event_name"]

        chrpsychs_av_interview_date = row["chrpsychs_av_interview_date"]
        interview_date = datetime.datetime.strptime(
            chrpsychs_av_interview_date, "%Y-%m-%d"
        )

        consent_date = row["consent_date"]

        # Convert consent_date to datetime.datetime for compatibility
        if isinstance(consent_date, str):
            consent_date = datetime.datetime.strptime(consent_date, "%Y-%m-%d")
        elif isinstance(consent_date, datetime.date):
            consent_date = datetime.datetime.combine(
                consent_date, datetime.datetime.min.time()
            )

        days = dpdash.get_days_between_dates(
            consent_date=consent_date, event_date=interview_date
        )

        interview_name = dpdash.get_dpdash_name(
            subject=subject_id,
            study=study_id,
            data_type="psychsInterview",
            consent_date=consent_date,
            event_date=interview_date,
        )

        expected_interview = ExpectedInterview(
            interview_name=interview_name,
            subject_id=subject_id,
            study_id=study_id,
            form_name=form_name,
            event_name=event_name,
            expected_interview_date=interview_date,
            expected_interview_day=days,
            expected_interview_type="psychs",
        )

        expected_interviews.append(expected_interview)

    return expected_interviews


def models_to_db(
    expected_interviews: List[ExpectedInterview], config_file: Path
) -> None:
    """
    Imports the ExpectedInterview models into the database.

    Args:
        expected_interviews (List[ExpectedInterview]): The list of ExpectedInterview models.
        config_file (Path): The path to the config file.

    Returns:
        None
    """
    sql_queries = []

    for expected_interview in expected_interviews:
        sql_queries.append(expected_interview.to_sql())

    db.execute_queries(
        queries=sql_queries,
        config_file=config_file,
        show_commands=False,
    )


def import_expected_interviews(
    study: str,
    config_file: Path,
) -> None:
    """
    Imports expected interviews from the form data.

    Args:
        data_root (Path): The root path of the data.
        study (str): The study name.
        config_file (Path): The path to the config file.
    """

    expected_interviews: List[ExpectedInterview] = []

    open_interviews = get_open_interviews(study=study, config_file=config_file)
    expected_interviews.extend(open_interviews)

    psychs_interviews = get_psychs_interviews(study=study, config_file=config_file)
    expected_interviews.extend(psychs_interviews)

    logger.info(
        f"Found {len(expected_interviews)} expected interviews for study {study}."
    )
    if len(expected_interviews) > 0:
        logger.info(
            f"{study}: {len(open_interviews)} open interviews, {len(psychs_interviews)} psychs interviews."
        )

    models_to_db(expected_interviews=expected_interviews, config_file=config_file)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog=MODULE_NAME, description="Import exported interviews."
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

    studies = orchestrator.get_studies(config_file=config_file)

    for study in studies:
        logger.info(f"Processing study: {study}")
        import_expected_interviews(study=study, config_file=config_file)

    logger.info("[bold green]Done!", extra={"markup": True})
