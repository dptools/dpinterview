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

import logging
import argparse
from typing import List, Dict, Any
import json
from datetime import datetime
import multiprocessing

from rich.logging import RichHandler

from pipeline import orchestrator
from pipeline.helpers import cli, utils, db
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


def parse_form_json(subject_form_json: Path) -> List[FormData]:
    """
    Parses the form JSON file and returns a list of FormData objects.

    Args:
        subject_form_json (Path): The path to the form JSON file.

    Returns:
        List[FormData]: A list of FormData objects.
    """
    with open(subject_form_json, "r", encoding="utf-8") as f:
        subject_data = json.load(f)

    results: List[Dict[str, Any]] = []

    for event in subject_data:
        event_name = event["redcap_event_name"]
        required_prefix = "chrpsychs_av"

        matched_keys = [key for key in event.keys() if key.startswith(required_prefix)]

        event_data = {
            key: event[key] for key in matched_keys if event[key] not in ("", None)
        }

        if not event_data:
            continue

        result = {
            "subject_id": event["chric_record_id"],
            "form_name": "psychs_av_recording_run_sheet",
            "event_name": event_name,
            "event_data": event_data,
        }
        results.append(result)

    for event in subject_data:
        event_name = event["redcap_event_name"]
        required_prefix = ["chrspeech", "chr_speech"]

        matched_keys = [
            key
            for key in event.keys()
            if any(key.startswith(prefix) for prefix in required_prefix)
        ]

        event_data = {
            key: event[key] for key in matched_keys if event[key] not in ("", None)
        }

        if not event_data:
            continue

        result = {
            "subject_id": event["chric_record_id"],
            "form_name": "speech_sampling_run_sheet",
            "event_name": event_name,
            "event_data": event_data,
        }
        results.append(result)

    form_data_list = []

    # Get the source modified date from the JSON file
    source_mdata = subject_form_json.stat().st_mtime
    source_mdata_dt = datetime.fromtimestamp(source_mdata)

    for result in results:
        subject_id = result["subject_id"]
        study_id = f"Pronet{subject_id[:2]}"

        form_data = FormData(
            subject_id=subject_id,
            study_id=study_id,
            form_name=result["form_name"],
            event_name=result["event_name"],
            form_data=result["event_data"],
            source_mdata=source_mdata_dt,
        )
        form_data_list.append(form_data)

    return form_data_list


def models_to_db(
    form_data_list: List[FormData], config_file: Path
) -> None:
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
    study: str,
    config_file: Path,
) -> None:
    """
    Imports form data (typically REDCap JSONs) into the database.

    Args:
        data_root (Path): The root directory of the data.
        study (str): The study name.
        config_file (Path): The path to the config file.
    """
    subjects_root = data_root / "PROTECTED" / study

    crawler_params = utils.config(path=config_file, section="crawler")
    form_data_study_pattern = crawler_params["form_data_study_pattern"]
    form_data_study_pattern = form_data_study_pattern.strip()

    form_jsons = list(subjects_root.glob(form_data_study_pattern))
    logger.info(f"Found {len(form_jsons)} form JSON files.")

    num_processes = multiprocessing.cpu_count() // 4
    logger.info(f"Using {num_processes} processes for parsing.")
    form_data_list = []

    with multiprocessing.Pool(processes=num_processes) as pool:
        with utils.get_progress_bar() as progress:
            task = progress.add_task(
                "[cyan]Parsing form JSON files...", total=len(form_jsons)
            )
            for result in pool.imap_unordered(
                parse_form_json, form_jsons
            ):
                form_data_list.extend(result)
                progress.update(task, advance=1)

    logger.info(f"Found {len(form_data_list)} form data entries.")

    models_to_db(
        form_data_list=form_data_list,
        config_file=config_file,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog=MODULE_NAME,
        description="Import survey / form data (typically REDCap JSONs) into the database.",
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

    data_root = orchestrator.get_data_root(config_file=config_file, enforce_real=True)
    studies = orchestrator.get_studies(config_file=config_file)

    for study in studies:
        logger.info(f"Processing study: {study}")
        import_form_data(data_root=data_root, study=study, config_file=config_file)

    logger.info("[bold green]Done!", extra={"markup": True})
