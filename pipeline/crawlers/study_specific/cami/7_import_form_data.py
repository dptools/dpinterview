#!/usr/bin/env python
"""
Imports CAMI AV runsheet data from consolidated REDCap CSV into form_data table.
Handles both MD and RA checklist fields from a single study-level file.
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
import pandas as pd
from datetime import datetime
from typing import List, Optional

from rich.logging import RichHandler

from pipeline import orchestrator
from pipeline.helpers import cli, utils, db
from pipeline.models.form_data import FormData

MODULE_NAME = "import_form_data"

logger = logging.getLogger(MODULE_NAME)
logargs = {
    "level": logging.DEBUG,
    "format": "%(message)s",
    "handlers": [RichHandler(rich_tracebacks=True)],
}
logging.basicConfig(**logargs)

console = utils.get_console()

# MD fields: all columns ending with _md (excluding completeness flag)
MD_FORM_NAME = "av_check_md"
RA_FORM_NAME = "av_check_ra"

# Only process inpatient session rows — onboarding and php30 are empty
TARGET_EVENT = "inpatient_session_arm_1"


def parse_redcap_date(date_str: str) -> Optional[datetime]:
    """
    Parses REDCap date formats found in CAMI AV runsheet:
    - '9/21/23 13:01'   (primary format)
    - '9/21/2023 1:01:00 PM'  (fallback)
    - '9/21/23'  (date only fallback)
    """
    if not date_str or str(date_str).strip() in ("", "nan", "NaN"):
        return None

    date_str = str(date_str).strip()

    formats = [
        "%m/%d/%y %H:%M",       # 9/21/23 13:01  ← primary
        "%m/%d/%Y %I:%M:%S %p", # 9/21/2023 1:01:00 PM
        "%m/%d/%y",             # 9/21/23
        "%m/%d/%Y",             # 9/21/2023
    ]

    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue

    logger.warning(f"Could not parse date: '{date_str}'")
    return None


def extract_form_data(
    row: pd.Series,
    subject_id: str,
    event_name: str,
    form_name: str,
    field_prefix: str,
    source_mdata_dt: datetime,
) -> Optional[FormData]:
    """
    Extracts fields for a given form from a row and returns a FormData object.
    Only includes non-empty fields that match the given prefix.

    Args:
        row: A pandas Series representing one row of the CSV.
        subject_id: The subject ID.
        event_name: The unique event identifier.
        form_name: The form name (av_check_md or av_check_ra).
        field_prefix: Column prefix to filter on ('_md' for MD, '' for RA).
        source_mdata_dt: File modification datetime.

    Returns:
        FormData object or None if no relevant fields found.
    """
    event_data = {}

    for col, val in row.items():
        # Skip metadata columns
        if col in (
            "record_id",
            "redcap_event_name",
            "redcap_repeat_instrument",
            "redcap_repeat_instance",
            "redcap_survey_identifier",
        ):
            continue

        # Skip empty values
        if pd.isna(val) or str(val).strip() in ("", "nan", "NaN"):
            continue

        # For MD form: include columns ending with _md or _md___N
        # For RA form: include columns that do NOT end with _md or _md___N
        col_lower = col.lower()
        is_md_col = col_lower.endswith("_md") or "_md___" in col_lower or \
                    col_lower.endswith("_md_complete") or "avqc_redcap_user_md" in col_lower or \
                    col_lower.endswith("_md_v2") or "upload_date_md" in col_lower or \
                    "action_md" in col_lower or "check_md" in col_lower

        if field_prefix == "_md" and not is_md_col:
            continue
        if field_prefix == "" and is_md_col:
            continue

        # Convert numpy types to native Python
        val = val.item() if hasattr(val, "item") else val
        event_data[col] = val

    if not event_data:
        return None

    return FormData(
        subject_id=subject_id,
        study_id="CAMI",
        form_name=form_name,
        event_name=event_name,
        form_data=event_data,
        source_mdata=source_mdata_dt,
    )


def parse_runsheet_csv(csv_path: Path) -> List[FormData]:
    """
    Parses the consolidated CAMI AV runsheet CSV and returns
    a list of FormData objects for both MD and RA forms.

    Args:
        csv_path: Path to the consolidated REDCap CSV file.

    Returns:
        List of FormData objects.
    """
    source_mdata_dt = datetime.fromtimestamp(csv_path.stat().st_mtime)

    try:
        df = pd.read_csv(csv_path, dtype=str)
    except Exception as e:
        logger.error(f"Failed to read {csv_path}: {e}")
        return []

    logger.info(f"Read {len(df)} rows from {csv_path.name}")

    # Filter to inpatient session rows only
    df = df[df["redcap_event_name"] == TARGET_EVENT].copy()
    logger.info(f"Found {len(df)} inpatient_session_arm_1 rows after filtering")

    if df.empty:
        logger.warning("No inpatient session rows found.")
        return []

    form_data_list: List[FormData] = []

    for _, row in df.iterrows():
        subject_id = str(row["record_id"]).strip()
        repeat_instance = str(row.get("redcap_repeat_instance", "1")).strip()
        event_name = f"{TARGET_EVENT}_instance_{repeat_instance}"

        # Check MD interview date exists before processing MD form
        md_date_str = row.get("avcheck_interview_date_md", "")
        md_date = parse_redcap_date(md_date_str)

        if md_date is not None:
            md_form = extract_form_data(
                row=row,
                subject_id=subject_id,
                event_name=event_name,
                form_name=MD_FORM_NAME,
                field_prefix="_md",
                source_mdata_dt=source_mdata_dt,
            )
            if md_form:
                form_data_list.append(md_form)
            else:
                logger.warning(
                    f"No MD fields found for {subject_id} instance {repeat_instance}"
                )
        else:
            logger.debug(
                f"No MD interview date for {subject_id} instance {repeat_instance} — skipping MD form"
            )

        # Check RA interview date exists before processing RA form
        ra_date_str = row.get("avcheck_interview_date", "")
        ra_date = parse_redcap_date(ra_date_str)

        if ra_date is not None:
            ra_form = extract_form_data(
                row=row,
                subject_id=subject_id,
                event_name=event_name,
                form_name=RA_FORM_NAME,
                field_prefix="",
                source_mdata_dt=source_mdata_dt,
            )
            if ra_form:
                form_data_list.append(ra_form)
            else:
                logger.warning(
                    f"No RA fields found for {subject_id} instance {repeat_instance}"
                )
        else:
            logger.debug(
                f"No RA interview date for {subject_id} instance {repeat_instance} — skipping RA form"
            )

    logger.info(
        f"Produced {len(form_data_list)} form_data entries "
        f"({sum(1 for f in form_data_list if f.form_name == MD_FORM_NAME)} MD, "
        f"{sum(1 for f in form_data_list if f.form_name == RA_FORM_NAME)} RA)"
    )

    return form_data_list


def models_to_db(form_data_list: List[FormData], config_file: Path) -> None:
    """
    Writes FormData objects to the database.
    """
    if not form_data_list:
        logger.warning("No form data to write.")
        return

    sql_queries = [fd.to_sql() for fd in form_data_list]
    logger.info(f"Writing {len(sql_queries)} rows to form_data table.")
    db.execute_queries(
        queries=sql_queries,
        config_file=config_file,
        show_commands=False,
        show_progress=True,
    )


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

    # Read av_runsheet_path from config
    crawler_params = utils.config(path=config_file, section="crawler")
    av_runsheet_path = Path(crawler_params["av_runsheet_path"].strip())

    if not av_runsheet_path.exists():
        logger.error(f"AV runsheet file not found: {av_runsheet_path}")
        sys.exit(1)

    logger.info(f"Reading AV runsheet from: {av_runsheet_path}")

    form_data_list = parse_runsheet_csv(csv_path=av_runsheet_path)
    models_to_db(form_data_list=form_data_list, config_file=config_file)

    logger.info("[bold green]Done!", extra={"markup": True})