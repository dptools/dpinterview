#!/usr/bin/env python
"""
Finds CAMI interviews from imported form_data and populates expected_interviews.
Handles both MD and RA interview types.
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

try:
    sys.path.remove(str(parent))
except ValueError:
    pass

import logging
import argparse
import datetime
from typing import List, Optional

from rich.logging import RichHandler

from pipeline import orchestrator
from pipeline.helpers import cli, utils, db, dpdash
from pipeline.models.expected_interviews import ExpectedInterview

MODULE_NAME = "import_expected_interviews"

logger = logging.getLogger(MODULE_NAME)
logargs = {
    "level": logging.DEBUG,
    "format": "%(message)s",
    "handlers": [RichHandler(rich_tracebacks=True)],
}
logging.basicConfig(**logargs)

console = utils.get_console()


def lookup_interview_name(
    subject_id: str,
    interview_date: datetime.datetime,
    config_file: Path,
) -> Optional[str]:
    """
    Looks up the exact interview_name from the interviews table by matching
    subject_id and interview date. Uses the DB directly to avoid any
    day-number generation issues.

    Args:
        subject_id: The subject ID.
        interview_date: The interview date from form_data (osi_date).
        config_file: Path to the config file.

    Returns:
        The exact interview_name string if found, None otherwise.
    """
    query = f"""
    SELECT i.interview_name
    FROM interview_parts ip
    JOIN interviews i USING (interview_name)
    WHERE ip.interview_datetime::date = '{interview_date.date()}'
        AND i.subject_id = '{subject_id}'
        AND i.study_id = 'CAMI'
        AND ip.is_primary = TRUE
    LIMIT 1
    """

    interview_name = db.fetch_record(config_file=config_file, query=query)

    if interview_name is None:
        logger.warning(
            f"No interview found in DB for subject {subject_id} "
            f"on {interview_date.date()}. Skipping."
        )

    return interview_name


def get_interviews_for_form(
    study: str,
    form_name: str,
    config_file: Path,
) -> List[ExpectedInterview]:
    """
    Queries form_data for a given form_name and returns a list of
    ExpectedInterview objects. Used for both MD_interviews and RA_interviews.

    Args:
        study: The study ID (CAMI).
        form_name: Either "MD_interviews" or "RA_interviews".
        config_file: Path to the config file.

    Returns:
        List of ExpectedInterview objects.
    """
    expected_interviews: List[ExpectedInterview] = []

    sql_query = f"""
    SELECT
        subject_id,
        study_id,
        form_name,
        event_name,
        CASE
            WHEN form_name = 'av_check_md'
                THEN form_data ->> 'avcheck_interview_date_md'
            ELSE
                form_data ->> 'avcheck_interview_date'
        END AS interview_date,
        consent_date
    FROM public.form_data
    LEFT JOIN subjects USING (subject_id, study_id)
    WHERE form_name = '{form_name}'
        AND study_id = '{study}'
        AND (
            CASE
                WHEN form_name = 'av_check_md'
                    THEN form_data ->> 'avcheck_interview_date_md'
                ELSE
                    form_data ->> 'avcheck_interview_date'
            END
        ) IS NOT NULL
    """

    result_df = db.execute_sql(config_file=config_file, query=sql_query)

    if result_df.empty:
        logger.warning(f"No form_data rows found for form_name='{form_name}' in study '{study}'.")
        return expected_interviews

    logger.info(f"Found {len(result_df)} rows in form_data for form_name='{form_name}'.")

    for _, row in result_df.iterrows():
        subject_id = row["subject_id"]
        study_id   = row["study_id"]
        form_name_ = row["form_name"]
        event_name = row["event_name"]

        # Parse interview date
        formats = [
            "%m/%d/%y %H:%M",
            "%m/%d/%Y %I:%M:%S %p",
            "%m/%d/%y",
            "%m/%d/%Y",
        ]

        interview_date = None
        for fmt in formats:
            try:
                interview_date = datetime.datetime.strptime(
                    row["interview_date"].strip(), fmt
                )
                break
            except ValueError:
                continue

        if interview_date is None:
            logger.error(
                f"Could not parse interview_date '{row['interview_date']}' "
                f"for {subject_id}. Skipping."
            )
            continue

        # Parse consent date
        consent_date = row["consent_date"]
        if isinstance(consent_date, str):
            consent_date = datetime.datetime.strptime(consent_date, "%Y-%m-%d")
        elif isinstance(consent_date, datetime.date):
            consent_date = datetime.datetime.combine(
                consent_date, datetime.datetime.min.time()
            )

        # Compute days since consent (for the expected_interview_day column)
        days = dpdash.get_days_between_dates(
            consent_date=consent_date, event_date=interview_date
        )

        # Look up the exact interview_name from the DB
        # This avoids the day-number bug in dpdash.get_dpdash_name()
        interview_name = lookup_interview_name(
            subject_id=subject_id,
            interview_date=interview_date,
            config_file=config_file,
        )

        if interview_name is None:
            # No matching interview found in the pipeline DB for this date
            # This means crawler 2 hasn't processed this interview yet,
            # or the recording doesn't exist on disk
            continue

        # interview_type distinguishes MD from RA in the expected_interviews table
        interview_type = "onsite_md" if form_name_ == "av_check_md" else "onsite_ra"

        expected_interview = ExpectedInterview(
            interview_name=interview_name,
            subject_id=subject_id,
            study_id=study_id,
            form_name=form_name_,
            event_name=event_name,
            expected_interview_date=interview_date,
            expected_interview_day=days,
            expected_interview_type=interview_type,
        )
        expected_interviews.append(expected_interview)

    return expected_interviews


def models_to_db(
    expected_interviews: List[ExpectedInterview],
    study_id: str,
    config_file: Path,
) -> None:
    """
    Writes expected_interviews to the DB.
    Truncates existing CAMI rows first to ensure a clean re-run.
    """
    sql_queries = [ExpectedInterview.truncate_by_study_query(study=study_id)]

    for ei in expected_interviews:
        sql_queries.append(ei.to_sql())

    db.execute_queries(
        queries=sql_queries,
        config_file=config_file,
        show_commands=False,
    )


def import_expected_interviews(study: str, config_file: Path) -> None:
    """
    Main entry point. Processes both MD and RA form data and writes
    all expected interviews to the DB in one batch.
    """
    all_expected: List[ExpectedInterview] = []

    for form_name in ["av_check_ra", "av_check_md"]:
        logger.info(f"Processing form: {form_name}")
        interviews = get_interviews_for_form(
            study=study,
            form_name=form_name,
            config_file=config_file,
        )
        logger.info(
            f"Found {len(interviews)} expected interviews for {form_name}."
        )
        all_expected.extend(interviews)

    logger.info(
        f"Total expected interviews for {study}: {len(all_expected)} "
        f"({sum(1 for e in all_expected if e.expected_interview_type == 'onsite_md')} MD, "
        f"{sum(1 for e in all_expected if e.expected_interview_type == 'onsite_ra')} RA)."
    )

    if all_expected:
        models_to_db(
            expected_interviews=all_expected,
            study_id=study,
            config_file=config_file,
        )
    else:
        logger.warning(f"No expected interviews found for {study}. Nothing written to DB.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog=MODULE_NAME)
    parser.add_argument("-c", "--config", type=str, required=False)
    args = parser.parse_args()

    if args.config:
        config_file = Path(args.config).resolve()
        if not config_file.exists():
            logger.error(f"Config file '{config_file}' does not exist.")
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