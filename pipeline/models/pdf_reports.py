#!/usr/bin/env python
"""
PdfReport Model
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
from typing import Optional

from pipeline.helpers import utils, db

console = utils.get_console()


class PdfReport:
    """
    Represents a row in the 'pdf_reports' table.

    Attributes:
        interview_name (str): The name of the interview.
        pr_version (str): The version of the report.
        pr_path (str): The path to the report.
        pr_generation_time (float): The time it took to generate the report.
        pr_timestamp (datetime): The timestamp of the report generation.
    """

    def __init__(
        self,
        interview_name: str,
        pr_version: str,
        pr_path: str,
        pr_timestamp: datetime,
        pr_generation_time: Optional[float] = None,
    ):
        self.interview_name = interview_name
        self.pr_version = pr_version
        self.pr_path = pr_path
        self.pr_generation_time = pr_generation_time
        self.pr_timestamp = pr_timestamp

    def __repr__(self):
        return f"""PdfReport(
    interview_name={self.interview_name},
    pr_version={self.pr_version},
    pr_path={self.pr_path},
    pr_generation_time={self.pr_generation_time},
    pr_timestamp={self.pr_timestamp}
)
"""

    def __str__(self):
        return self.__repr__()

    @staticmethod
    def init_table_query() -> str:
        """
        Return the SQL query to create the 'pdf_reports' table.
        """
        sql_query = """
        CREATE TABLE IF NOT EXISTS pdf_reports (
            interview_name TEXT NOT NULL REFERENCES load_openface(interview_name),
            pr_version TEXT NOT NULL,
            pr_path TEXT NOT NULL,
            pr_generation_time REAL NOT NULL,
            pr_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (interview_name, pr_version)
        );
        """
        return sql_query

    @staticmethod
    def drop_table_query() -> str:
        """
        Return the SQL query to drop the 'pdf_reports' table.
        """
        sql_query = """
        DROP TABLE IF EXISTS pdf_reports;
        """
        return sql_query

    def to_sql(self) -> str:
        """
        Return the SQL query to insert the object into the 'pdf_reports' table.
        """
        if self.pr_generation_time is None:
            self.pr_generation_time = 0.0

        sql_query = f"""
        INSERT INTO pdf_reports (interview_name, pr_version,
            pr_path, pr_generation_time)
        VALUES ('{self.interview_name}', '{self.pr_version}',
            '{self.pr_path}', {self.pr_generation_time});
        """

        sql_query = db.handle_null(sql_query)

        return sql_query


if __name__ == "__main__":
    config_file = utils.get_config_file_path()

    console.log("Initializing 'pdf_reports' table...")
    console.log("[red]This will delete all existing data in the 'pdf_reports' table!")

    drop_queries = [PdfReport.drop_table_query()]
    create_queries = [PdfReport.init_table_query()]

    sql_queries = drop_queries + create_queries

    db.execute_queries(config_file=config_file, queries=sql_queries, show_commands=True)

    console.log("'pdf_reports' table initialized.")
