#!/usr/bin/env python
"""
Loads the study metadata into the database.

Populates the 'study' and 'subject' tables with the study metadata CSV file.
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
from typing import List

import pandas as pd
from rich.logging import RichHandler

from pipeline import orchestrator
from pipeline.helpers import cli, db, utils
from pipeline.helpers.config import config
from pipeline.models.study import Study
from pipeline.models.subjects import Subject

MODULE_NAME = "import_study_metadata"
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


def insert_study(config_file: Path, study_id: str) -> None:
    """
    Inserts the study into the database.

    Args:
        config_file (Path): The path to the configuration file.
        study_id (str): The ID of the study.

    Returns:
        None
    """
    study = Study(study_id=study_id)

    logger.info(f"Inserting study: {study_id}")
    query = study.to_sql()

    db.execute_queries(config_file=config_file, queries=[query])


def get_study_metadata(config_file: Path, study_id: str) -> pd.DataFrame:
    """
    Gets the study metadata from the PHOENIX structure

    Args:
        config_file (str): The path to the configuration file.
        study_id (str): The ID of the study.

    Returns:
        pd.DataFrame: The study metadata.

    Raises:
        FileNotFoundError: If the study metadata file is not found.
    """

    # Construct path to study metadata
    params = config(path=config_file, section="general")
    data_root = params["data_root"]

    metadata_filename = f"{study_id}_metadata.csv"

    study_metadata = Path(data_root, "GENERAL", study_id, metadata_filename)

    # Check if study_metadata exists
    if not study_metadata.exists():
        logger.error(f'Study metadata file "{study_metadata}" not found.')
        raise FileNotFoundError(f"could not read file: {study_metadata}")
    else:
        insert_study(config_file=config_file, study_id=study_id)

    # Read study metadata
    study_metadata = pd.read_csv(study_metadata)

    return study_metadata


def fetch_subjects(config_file: Path, study_id: str) -> List[Subject]:
    """
    Fetches the subjects from the study metadata.

    Args:
        config_file (Path): The path to the configuration file.
        study_id (str): The ID of the study.
    """

    # Get study metadata
    study_metadata = get_study_metadata(config_file=config_file, study_id=study_id)

    subjects: List[Subject] = []
    for _, row in study_metadata.iterrows():
        # Get required fields
        required_fields = ["Subject ID", "Active", "Consent", "Study"]

        # Drop rows with NaN in required fields
        if pd.isna(row[required_fields]).any():
            continue

        subject_id = row["Subject ID"]
        active = row["Active"]  # 1 if active, 0 if inactive
        consent = row["Consent"]

        # Cast to bool
        active = bool(active)

        # Cast to DateTime
        consent = datetime.strptime(consent, "%Y-%m-%d")

        # construct optional_notes field as a JSON object
        # with all other fields as key-value pairs
        optional_notes = {}
        for field in study_metadata.columns:
            if field in required_fields:
                continue
            # Skip if NaN
            if pd.isna(row[field]):
                continue
            optional_notes[field] = row[field]

        subject = Subject(
            study_id=study_id,
            subject_id=subject_id,
            is_active=active,
            consent_date=consent,
            optional_notes=optional_notes,
        )

        subjects.append(subject)

    logger.info(f"Found {len(subjects)} subjects.")
    return subjects


def insert_subjects(config_file: Path, subjects: List[Subject]):
    """
    Inserts the subjects into the database.

    Args:
        config_file (Path): The path to the configuration file.
        subjects (List[Subject]): The list of subjects to insert.
    """

    queries = [subject.to_sql() for subject in subjects]

    db.execute_queries(config_file=config_file, queries=queries, show_commands=False)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog=MODULE_NAME, description="Gather metadata for files."
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

    for study_id in studies:
        logger.info(f"Study ID: {study_id}")
        subjects = fetch_subjects(config_file=config_file, study_id=study_id)
        insert_subjects(config_file=config_file, subjects=subjects)

    logger.info("[bold green]Done!", extra={"markup": True})
