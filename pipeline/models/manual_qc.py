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
from typing import Any, Dict

from pipeline.helpers import utils, db

console = utils.get_console()


class ManualQC:
    """
    Interview-specific manual QC.

    Attributes:
        interview_id (int): The interview ID.
        qc_comments (str): The manual QC comments.
        qc_data (dict): The manual QC data.
        qc_user_id (str): The ID of the user who performed the manual QC.
        qc_timestamp (datetime): The timestamp of when the manual QC was
    """

    def __init__(
        self,
        interview_id: int,
        qc_comments: str,
        qc_data: Dict[str, Any],
        qc_user_id: str,
        qc_timestamp: datetime,
    ):
        self.interview_id = interview_id
        self.qc_comments = qc_comments
        self.qc_data = qc_data
        self.qc_user_id = qc_user_id
        self.qc_timestamp = qc_timestamp

    def __repr__(self):
        return f"""ManualQc(
    interview_id={self.interview_id},
    qc_data={self.qc_data},
    qc_comments={self.qc_comments},
    qc_user_id={self.qc_user_id},
    qc_timestamp={self.qc_timestamp}
)"""

    def __str__(self):
        return self.__repr__()

    @staticmethod
    def init_table_query() -> str:
        """
        Return the SQL query to create the 'manual_qc' table.
        """
        sql_query = """
        CREATE TABLE IF NOT EXISTS manual_qc (
            interview_id INTEGER PRIMARY KEY,
            qc_comments TEXT,
            qc_data JSONB,
            qc_user_id TEXT NOT NULL,
            qc_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        """
        return sql_query

    @staticmethod
    def drop_table_query() -> str:
        """
        Return the SQL query to drop the 'manual_qc' table.
        """
        sql_query = """
        DROP TABLE IF EXISTS manual_qc;
        """
        return sql_query

    @staticmethod
    def get_row_query(interview_id: str) -> str:
        """
        Return the SQL query to get a row from the 'manual_qc' table.

        Args:
            interview_id (str): ID of the interview

        Returns:
            str: SQL query to get the row
        """
        sql_query = f"""
        SELECT * FROM manual_qc
        WHERE interview_id = '{interview_id}';
        """
        return sql_query

    @staticmethod
    def drop_row_query(interview_id: str) -> str:
        """
        Return the SQL query to drop a row from the 'manual_qc' table.

        Args:
            interview_name (str): Name of the interview

        Returns:
            str: SQL query to delete the row
        """
        sql_query = f"""
        DELETE FROM manual_qc
        WHERE interview_id = '{interview_id}';
        """
        return sql_query

    def to_sql(self) -> str:
        """
        Return the SQL query to insert the object into the 'manual_qc' table.
        """

        qc_data_json = db.sanitize_json(self.qc_data)

        sql_query = f"""
        INSERT INTO manual_qc (
            interview_id, qc_comments, qc_data,
            qc_user_id, qc_timestamp
        ) VALUES (
            {self.interview_id}, '{self.qc_comments}', '{qc_data_json}',
            '{self.qc_user_id}', '{self.qc_timestamp}'
        ) ON CONFLICT (interview_id) DO UPDATE SET
            qc_comments = '{self.qc_comments}',
            qc_data = '{qc_data_json}',
            qc_user_id = '{self.qc_user_id}',
            qc_timestamp = '{self.qc_timestamp}';
        """

        return sql_query


if __name__ == "__main__":
    config_file = utils.get_config_file_path()

    console.log("Initializing 'manual_qc' table...")
    console.log("[red]This will delete all existing data in the 'manual_qc' table!")

    drop_queries = [ManualQC.drop_table_query()]
    create_queries = [ManualQC.init_table_query()]

    sql_queries = drop_queries + create_queries

    db.execute_queries(config_file=config_file, queries=sql_queries, show_commands=True)

    console.log("'manual_qc' table initialized.")
