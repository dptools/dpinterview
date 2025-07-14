#!/usr/bin/env python
"""
FacepipeRun Model
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

from datetime import datetime
from typing import Dict, Any

from pipeline.helpers import db, utils

console = utils.get_console()


class FacepipeRun:
    """
    Represents a Facepipe run.
    """

    def __init__(
        self,
        fp_source_video_path: Path,
        fp_features_csv_path: Path,
        fp_run_metadata: Dict[str, Any],
        fp_duration_s: int,
        fp_timestamp: datetime,
    ):
        self.fp_source_video_path = fp_source_video_path
        self.fp_features_csv_path = fp_features_csv_path
        self.fp_run_metadata = fp_run_metadata
        self.fp_duration_s = fp_duration_s
        self.fp_timestamp = fp_timestamp

    def __repr__(self) -> str:
        return f"FacepipeRun({self.fp_source_video_path})"

    def __str__(self):
        return self.__repr__()

    @staticmethod
    def init_table_query() -> str:
        """
        Returns the SQL query to initialize the FacepipeRun table.
        """
        init_table_query: str = """
        CREATE TABLE IF NOT EXISTS facepipe.facepipe_runs (
            fp_source_video_path TEXT REFERENCES video_quick_qc(video_path) ON DELETE CASCADE NOT NULL PRIMARY KEY,
            fp_features_csv_path TEXT NOT NULL,
            fp_run_metadata JSONB NOT NULL,
            fp_duration_s INTEGER NOT NULL,
            fp_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        """

        return init_table_query

    @staticmethod
    def drop_table_query() -> str:
        """
        Returns the SQL query to drop the FacepipeRun table.
        """
        drop_table_query: str = """
        DROP TABLE IF EXISTS facepipe.facepipe_runs;
        """

        return drop_table_query

    def to_sql(self) -> str:
        """
        Returns the SQL query to insert this FacepipeRun instance into the database.
        """

        sanitized_fp_run_metadata = db.sanitize_json(self.fp_run_metadata)

        sql_query: str = f"""
        INSERT INTO facepipe.facepipe_runs (
            fp_source_video_path,
            fp_features_csv_path,
            fp_run_metadata,
            fp_duration_s,
            fp_timestamp
        ) VALUES (
            '{self.fp_source_video_path}',
            '{self.fp_features_csv_path}',
            '{sanitized_fp_run_metadata}',
            {self.fp_duration_s},
            '{self.fp_timestamp}'
        );
        """

        return sql_query


if __name__ == "__main__":
    config_file = utils.get_config_file_path()

    console.log("Initializing `facepipe.facepipe_runs` table")
    console.log(
        "[red]This will delete all existing data in the 'facepipe.facepipe_runs' table!"
    )

    create_schema_query = "CREATE SCHEMA IF NOT EXISTS facepipe;"
    drop_queries = [FacepipeRun.drop_table_query()]
    create_queries = [create_schema_query, FacepipeRun.init_table_query()]

    sql_queries = drop_queries + create_queries

    db.execute_queries(config_file=config_file, queries=sql_queries, show_commands=True)

    console.log("'facepipe.facepipe_runs' table initialized.")
