#!/usr/bin/env python
"""
FauRoleValidation Model
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

from datetime import datetime
from typing import Any, Dict

from pipeline.helpers import db, utils

console = utils.get_console()


class FauRoleValidation:
    """
    Class representing the FAU Role Validation object

    Attributes:
        interview_name (str): Name of the interview
        fau_metrics (Dict[str, Any]): metrics related to FAU for
            determining role
        matches_with_transcript (bool): Whether the FAU metrics match
            with the transcript
        timestamp (datetime): Timestamp of the object creation
    """

    def __init__(
        self,
        interview_name: str,
        fau_metrics: Dict[str, Any],
        matches_with_transcript: bool,
    ):
        self.interview_name = interview_name
        self.fau_metrics = fau_metrics
        self.matches_with_transcript = matches_with_transcript
        self.timestamp: datetime = datetime.now()

    def __str__(self) -> str:
        return f"FauRoleValidation({self.interview_name}, \
{self.fau_metrics}, {self.matches_with_transcript})"

    def __repr__(self) -> str:
        return self.__str__()

    @staticmethod
    def init_table_query() -> str:
        """
        Returns the query to create the table in the database
        """
        frv_table_query = """
        CREATE TABLE IF NOT EXISTS fau_role_validation (
            interview_name TEXT REFERENCES load_openface(interview_name),
            frv_fau_metrics JSONB,
            frv_matches_with_transcript BOOLEAN,
            frv_timestamp TIMESTAMP,
            PRIMARY KEY (interview_name)
        )
        """

        return frv_table_query

    @staticmethod
    def drop_table_query() -> str:
        """
        Returns the query to drop the table in the database
        """
        frv_table_query = """
        DROP TABLE IF EXISTS fau_role_validation
        """

        return frv_table_query

    def to_sql(self) -> str:
        """
        Returns the query to insert the object into the database

        Returns:
            str: Query to insert the object into the database
        """

        fau_metrics = db.sanitize_json(self.fau_metrics)

        frv_insert_query = f"""
        INSERT INTO fau_role_validation (
            interview_name,
            frv_fau_metrics,
            frv_matches_with_transcript,
            frv_timestamp
        ) VALUES (
            '{self.interview_name}',
            '{fau_metrics}',
            {self.matches_with_transcript},
            '{self.timestamp}'
        )
        """

        return frv_insert_query


if __name__ == "__main__":
    config_file = utils.get_config_file_path()

    console.log("Initializing 'fau_role_validation' table")
    console.log(
        "[red]This will delete all existing data in the 'fau_role_validation' table!"
    )

    drop_queries = [FauRoleValidation.drop_table_query()]
    create_queries = [FauRoleValidation.init_table_query()]

    sql_queries = drop_queries + create_queries

    db.execute_queries(config_file=config_file, queries=sql_queries, show_commands=True)

    console.log("'fau_role_validation' table initialized.")
