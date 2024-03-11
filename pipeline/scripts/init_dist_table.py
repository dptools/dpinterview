#!/usr/bin/env python
"""
Initializes the Openface features table.
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

import logging
from typing import List
import os

from rich.logging import RichHandler

from pipeline.helpers import utils, db
from pipeline import data

MODULE_NAME = "init_db"
INSTANCE_NAME = MODULE_NAME

console = utils.get_console()


logger = logging.getLogger(MODULE_NAME)
logargs = {
    "level": logging.DEBUG,
    # "format": "%(asctime)s - %(process)d - %(name)s - %(levelname)s - %(message)s",
    "format": "%(message)s",
    "handlers": [RichHandler(rich_tracebacks=True)],
}
logging.basicConfig(**logargs)


def generate_drop_queries() -> List[str]:
    """
    Generates the drop queries for the OpenFace features table.

    Returns:
        List[str]: List of queries.
    """
    queries = [
        """
        DROP VIEW IF EXISTS openface_features_imported_view;
        """,
        """
        DROP TABLE IF EXISTS openface_features;
        """,
        """
        DROP INDEX IF EXISTS interview_name_index;
        """,
        """
        DROP INDEX IF EXISTS subject_id_index;
        """,
        """
        DROP INDEX IF EXISTS study_id_index;
        """,
        """
        DROP INDEX IF EXISTS ir_role_index;
        """,
        """
        DROP INDEX IF EXISTS off_timestamp_index;
        """,
    ]

    return queries


def generate_create_query(config_file: Path, csv_file: Path) -> str:
    """
    Generates the create query for the OpenFace features table., by using the
    sample csv file.

    Args:
        config_file (Path): Path to the config file.
        csv_file (Path): Path to the sample csv file (OpenFace output).
    """
    datatypes = data.get_openface_datatypes(config_file=config_file, csv_file=csv_file)

    query = """
        CREATE TABLE IF NOT EXISTS openface_features (
            interview_name TEXT NOT NULL,
            subject_id TEXT NOT NULL,
            study_id TEXT NOT NULL,
            ir_role TEXT NOT NULL,
    """

    # Note: Column names are case-sensitive in PostgreSQL when using double quotes
    #       Since OpenFace uses case sensitive column names, we will use double quotes
    for col, datatype in datatypes.items():
        query += f'"{col}" {datatype} NOT NULL,\n'

    query += """
            off_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (interview_name, ir_role, frame, face_id)
        );
    """

    return query


def finalize() -> List[str]:
    """
    Creates indexes and views, for the OpenFace features table.

    Returns:
        List[str]: List of queries.
    """
    queries = []

    index_queries = [
        """
        CREATE INDEX interview_name_index
        ON openface_features (interview_name);
        """,
        """
        CREATE INDEX subject_id_index
        ON openface_features (subject_id);
        """,
        """
        CREATE INDEX study_id_index
        ON openface_features (study_id);
        """,
        """
        CREATE INDEX ir_role_index
        ON openface_features (ir_role);
        """,
        """
        CREATE INDEX off_timestamp_index
        ON openface_features (off_timestamp);
        """,
    ]

    queries.extend(index_queries)

    view_queries = [
        # Create a view that shows osir_id of openface_features that have been imported
        """
        CREATE VIEW openface_features_imported_view AS
            SELECT DISTINCT interview_name
            FROM openface_features
            ORDER BY interview_name;
        """,
    ]

    queries.extend(view_queries)

    return queries


if __name__ == "__main__":
    config_file = utils.get_config_file_path()
    console.rule(f"[bold red]{MODULE_NAME}")
    logger.info(f"Using config file: {config_file}")

    openface_features_config = utils.config(
        path=config_file, section="openface_features"
    )
    csv_file = openface_features_config["sample_csv_file"]

    # Check if csv file exists
    if not os.path.isfile(csv_file):
        console.print(f"[red]CSV file '{csv_file}' does not exist.")
        sys.exit(1)

    # Check if csv file is empty
    if os.stat(csv_file).st_size == 0:
        console.print(f"[red]CSV file '{csv_file}' is empty.")
        sys.exit(1)

    queries = []

    # Drop queries
    drop_queries = generate_drop_queries()

    # Generate create query
    create_query = generate_create_query(
        config_file=config_file, csv_file=Path(csv_file)
    )

    # Finalize
    finalize_queries = finalize()

    queries.extend(drop_queries)
    queries.append(create_query)
    queries.extend(finalize_queries)

    # Execute queries
    db.execute_queries(config_file, queries, show_commands=True, db="openface_db")
