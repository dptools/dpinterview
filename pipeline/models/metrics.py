#!/usr/bin/env python
"""
OpenfaceQC Model
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

from pipeline.helpers import utils, db

console = utils.get_console()


class Metrics:
    """
    Study-specific metrics.

    Attributes:
        interview_name (int): The interview ID.
        metrics (Dict[str, Any]): The metrics.
        metrics_timestamp (datetime): The timestamp of when the metrics were
            generated.
    """

    def __init__(self, interview_name: str, metrics: dict):
        self.interview_name = interview_name
        self.metrics = metrics
        self.metrics_timestamp = datetime.now()

    def __repr__(self):
        return f"""Metrics(
    interview_name={self.interview_name},
    metrics={self.metrics},
    metrics_timestamp={self.metrics_timestamp}
)
"""

    def __str__(self):
        return self.__repr__()

    @staticmethod
    def init_table_query() -> str:
        """
        Return the SQL query to create the 'openface' table.
        """
        sql_query = """
        CREATE TABLE IF NOT EXISTS metrics (
            interview_name TEXT PRIMARY KEY,
            metrics JSONB,
            metrics_timestamp TIMESTAMP
        );
        """
        return sql_query

    @staticmethod
    def drop_table_query() -> str:
        """
        Return the SQL query to drop the 'openface' table.
        """
        sql_query = """
        DROP TABLE IF EXISTS metrics;
        """
        return sql_query

    def to_sql(self) -> str:
        """
        Return the SQL query to insert the object into the 'openface' table.
        """

        metrics_json = db.sanitize_json(self.metrics)

        sql_query = f"""
        INSERT INTO metrics
        (interview_name, metrics, metrics_timestamp)
        VALUES
        ('{self.interview_name}', '{metrics_json}', '{self.metrics_timestamp}')
        ON CONFLICT (interview_name)
        DO UPDATE
        SET
            metrics = '{metrics_json}',
            metrics_timestamp = '{self.metrics_timestamp}';
        """

        return sql_query


if __name__ == "__main__":
    config_file = utils.get_config_file_path()

    console.log("Initializing 'metrics' table...")
    console.log("[red]This will delete all existing data in the 'metrics' table!")

    drop_queries = [Metrics.drop_table_query()]
    create_queries = [Metrics.init_table_query()]

    sql_queries = drop_queries + create_queries

    db.execute_queries(config_file=config_file, queries=sql_queries, show_commands=True)

    console.log("'metrics' table initialized.")
