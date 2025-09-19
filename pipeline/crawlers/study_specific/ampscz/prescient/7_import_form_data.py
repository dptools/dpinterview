#!/usr/bin/env python
"""
Imports form data (typically REDCap JSONs) into the database.
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
from typing import Dict, List

import pandas as pd
from rich.logging import RichHandler

from pipeline.helpers import cli, db, utils
from pipeline.models.form_data import FormData

MODULE_NAME = "import_form_data"
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


def get_active_subjects(config_file: Path) -> List[str]:
    """
    Retrieves a list of active subject IDs from the database.

    Args:
        config_file (Path): The path to the config file.

    Returns:
        List[str]: A list of active subject IDs.
    """
    query = """
    SELECT subject_id
    FROM subjects
    """
    results_df = db.execute_sql(
        query=query,
        config_file=config_file,
    )
    active_subjects = results_df["subject_id"].tolist()

    return active_subjects


rpms_to_redcap_event: Dict[int, str] = {
    0: "unknown",
    1: "screening",
    2: "baseline",
    3: "month_1",
    4: "month_2",
    5: "month_3",
    6: "month_4",
    7: "month_5",
    8: "month_6",
    9: "month_7",
    10: "month_8",
    11: "month_9",
    12: "month_10",
    13: "month_11",
    14: "month_12",
    15: "month_18",
    16: "month_24",
    17: "other_study",
    22: "other_study",
    98: "conversion",
    99: "floating_forms",
    100: "screening",  # Self Consent
    101: "screening",  # Parental Consent
}


def parse_form_csv(
    form_csv: Path,
    form_name: str,
    config_file: Path,
) -> List[FormData]:
    """
    Parses the form CSV file and returns a list of FormData objects.

    Args:
        form_csv (Path): The path to the form CSV file.
        form_name (str): The name of the form.

    Returns:
        List[FormData]: A list of FormData objects.
    """
    form_data_list = []
    form_df = pd.read_csv(form_csv)

    subjects = form_df["subjectkey"].unique()
    active_subjects = get_active_subjects(config_file=config_file)

    subjects = [subject for subject in subjects if subject in active_subjects]

    for subject in subjects:
        subject_df = form_df[form_df["subjectkey"] == subject]

        for _, row in subject_df.iterrows():
            rpms_visit_id = row["visit"]
            event_name = rpms_to_redcap_event.get(rpms_visit_id, "unknown")

            subject_id = row["subjectkey"]
            study_id = f"Prescient{subject_id[:2]}"

            # Extract relevant columns starting with the form prefix
            if form_name == "psychs_av_recording_run_sheet":
                required_prefix = "chrpsychs_av"
            elif form_name == "speech_sampling_run_sheet":
                required_prefix = ["chrspeech", "chr_speech"]
            else:
                logger.warning(f"Unknown form name: {form_name}")
                continue

            if isinstance(required_prefix, list):
                matched_keys = [
                    key
                    for key in row.index
                    if any(key.startswith(prefix) for prefix in required_prefix)
                ]
            else:
                matched_keys = [
                    key for key in row.index if key.startswith(required_prefix)
                ]

            # event_data = {}
            # for key in matched_keys:
            #     value = row[key]
            #     if pd.isna(value) or value == "":
            #         continue

            #     # Try to cast to int if possible
            #     if isinstance(value, float) and value.is_integer():
            #         value = int(value)
            #     elif isinstance(value, str):
            #         try:
            #             value_int = int(value)
            #             value = value_int
            #         except ValueError:
            #             pass

            #     event_data[key] = value

            # if not event_data:
            #     continue

            event_data = {}
            for key in matched_keys:
                value = row[key]
                if pd.isna(value) or value == "":
                    continue

                # Try to cast to int if possible
                if isinstance(value, float) and value.is_integer():
                    value = int(value)
                elif isinstance(value, str):
                    try:
                        value_int = int(value)
                        value = value_int
                    except ValueError:
                        pass

                event_data[key] = str(value)

            if not event_data:
                continue

            # Cast date fields to string in YYYY-MM-DD format
            date_fields = [
                key
                for key in event_data
                if "date" in key.lower()
            ]
            for date_field in date_fields:
                # Prescient (Australia) uses MM/DD/YYYY format
                parsed_date = datetime.strptime(
                    event_data[date_field],
                    "%m/%d/%Y"
                )
                event_data[date_field] = parsed_date.strftime(
                    "%Y-%m-%d"
                )
                continue

            # Get the source modified date from the CSV file
            source_mdata = form_csv.stat().st_mtime
            source_mdata_dt = datetime.fromtimestamp(source_mdata)

            form_data = FormData(
                subject_id=subject_id,
                study_id=study_id,
                form_name=form_name,
                event_name=event_name,
                form_data=event_data,
                source_mdata=source_mdata_dt,
            )
            form_data_list.append(form_data)

    return form_data_list


def models_to_db(form_data_list: List[FormData], config_file: Path) -> None:
    """
    Imports the FormData models into the database.

    Args:
        form_data_list (List[FormData]): The list of FormData models.
        config_file (Path): The path to the config file.

    Returns:
        None
    """
    sql_queries = []

    for form_data in form_data_list:
        sql_queries.append(form_data.to_sql())

    db.execute_queries(
        queries=sql_queries,
        config_file=config_file,
        show_commands=False,
    )


def import_form_data(
    data_root: Path,
    config_file: Path,
) -> None:
    """
    Imports form data (typically REDCap JSONs) into the database.

    Args:
        data_root (Path): The root directory of the data.
        study (str): The study name.
        config_file (Path): The path to the config file.
    """
    forms_to_import: List[str] = [
        "psychs_av_recording_run_sheet",
        "speech_sampling_run_sheet",
    ]

    form_data_list = []

    for form_name in forms_to_import:
        form_pattern = data_root / f"*{form_name}_*.csv"
        form_csvs = list(form_pattern.parent.glob(form_pattern.name))

        with utils.get_progress_bar() as progress:
            task = progress.add_task(
                "[cyan]Parsing form CSV files...", total=len(form_csvs)
            )
            for form_csv in form_csvs:
                processed_form_data = parse_form_csv(
                    form_csv=form_csv, form_name=form_name, config_file=config_file
                )
                form_data_list.extend(processed_form_data)
                progress.update(task, advance=1)

    logger.info(f"Found {len(form_data_list)} form data entries.")

    models_to_db(
        form_data_list=form_data_list,
        config_file=config_file,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog=MODULE_NAME,
        description="Import survey / form data (RPMS CSVs) into the database.",
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

    forms_params = utils.config(config_file, section="forms")
    forms_root = forms_params.get("forms_root")
    if not forms_root:
        logger.error("Error: 'forms_root' not specified in the config file.")
        sys.exit(1)
    forms_root = Path(forms_root).resolve()
    logger.info(f"Using forms root: {forms_root}")

    import_form_data(data_root=forms_root, config_file=config_file)

    logger.info("[bold green]Done!", extra={"markup": True})
