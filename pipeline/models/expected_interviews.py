#!/usr/bin/env python
"""
ExpectedInterviews (expected_interviews) Model
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

from pydantic import BaseModel

from pipeline.helpers import utils, db

console = utils.get_console()


class ExpectedInterview(BaseModel):
    """
    ExpectedInterviews model.

    Attributes:
        interview_name (str): The name of the interview
        subject_id (str): The subject ID
        study_id (str): The study ID
        form_name (str): The form name
        event_name (str): The event name
        expected_interview_date (datetime): The expected interview date
        expected_interview_day (int): The expected interview day, from consent date
        expected_interview_type (str): The expected interview type
    """

    interview_name: str
    subject_id: str
    study_id: str
    form_name: str
    event_name: str
    expected_interview_date: datetime
    expected_interview_day: int
    expected_interview_type: str

    def __repr__(self):
        return f"""ExpectedInterviews (
    interview_name={self.interview_name},
    subject_id={self.subject_id},
    study_id={self.study_id},
    form_name={self.form_name},
    event_name={self.event_name},
    expected_interview_date={self.expected_interview_date},
    expected_interview_day={self.expected_interview_day},
    expected_interview_type={self.expected_interview_type}
)"""

    def __str__(self):
        return self.__repr__()

    @staticmethod
    def init_table_query() -> str:
        """
        Returns the SQL query to create the expected_interviews table.
        """
        sql_query = """
        CREATE TABLE expected_interviews (
            interview_name TEXT NOT NULL,
            subject_id TEXT NOT NULL,
            study_id TEXT NOT NULL,
            form_name TEXT NOT NULL,
            event_name TEXT NOT NULL,
            expected_interview_date TIMESTAMP NOT NULL,
            expected_interview_day INT NOT NULL,
            expected_interview_type TEXT NOT NULL,
            PRIMARY KEY (interview_name),
            FOREIGN KEY (subject_id, study_id, form_name, event_name)
                REFERENCES form_data (subject_id, study_id, form_name, event_name)
        )
        """

        return sql_query

    @staticmethod
    def drop_table_query() -> str:
        """
        Returns the SQL query to drop the expected_interviews table.
        """
        sql_query = """
        DROP TABLE IF EXISTS expected_interviews
        """

        return sql_query

    def to_sql(self) -> str:
        """
        Returns the SQL query to insert the expected interview data into the database.

        Returns:
            str: The SQL query to insert the expected interview data.
        """
        sql_query = f"""
        INSERT INTO expected_interviews (
            interview_name,
            subject_id,
            study_id,
            form_name,
            event_name,
            expected_interview_date,
            expected_interview_day,
            expected_interview_type
        ) VALUES (
            '{self.interview_name}',
            '{self.subject_id}',
            '{self.study_id}',
            '{self.form_name}',
            '{self.event_name}',
            '{self.expected_interview_date}',
            {self.expected_interview_day},
            '{self.expected_interview_type}'
        ) ON CONFLICT (interview_name) DO UPDATE SET
            subject_id = EXCLUDED.subject_id,
            study_id = EXCLUDED.study_id,
            form_name = EXCLUDED.form_name,
            event_name = EXCLUDED.event_name,
            expected_interview_date = EXCLUDED.expected_interview_date,
            expected_interview_day = EXCLUDED.expected_interview_day,
            expected_interview_type = EXCLUDED.expected_interview_type
        """
        return sql_query


if __name__ == "__main__":
    config_file = utils.get_config_file_path()

    console.log("Initializing 'expected_interviews' table...")
    console.log(
        "[red]This will delete all existing data in the 'expected_interviews' table!"
    )

    drop_queries = [ExpectedInterview.drop_table_query()]
    create_queries = [ExpectedInterview.init_table_query()]

    sql_queries = drop_queries + create_queries

    db.execute_queries(config_file=config_file, queries=sql_queries, show_commands=True)

    console.log("'expected_interviews' table initialized.")
