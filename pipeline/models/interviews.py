"""
Interview model
"""

from pathlib import Path
from datetime import datetime
from typing import Optional, List
from enum import Enum

from pipeline.helpers import db


class InterviewType(Enum):
    """
    Enumerates the types of interviews.
    """

    ONSITE = "onsite"
    OFFSITE = "offsite"
    OPEN = "open"
    PSYCHS = "psychs"

    @staticmethod
    def init_table_query() -> List[str]:
        """
        Return the SQL query to create the 'interview_types' table.
        """
        create_sql_query = """
        CREATE TABLE IF NOT EXISTS interview_types (
            interview_type TEXT NOT NULL PRIMARY KEY
        );
        """

        populate_sql_query = f"""
        INSERT INTO interview_types (interview_type)
        VALUES
            ('{InterviewType.ONSITE.value}'),
            ('{InterviewType.OFFSITE.value}'),
            ('{InterviewType.OPEN.value}'),
            ('{InterviewType.PSYCHS.value}')
        ON CONFLICT (interview_type) DO NOTHING;
        """

        sql_queries: List[str] = [create_sql_query, populate_sql_query]

        return sql_queries

    @staticmethod
    def drop_table_query() -> str:
        """
        Return the SQL query to drop the 'interview_types' table.
        """
        sql_query = """
        DROP TABLE IF EXISTS interview_types;
        """

        return sql_query


class Interview:
    """
    Represents an interview.

    Attributes:
        interview_id (int): The interview ID.
        interview_name (str): The name of the interview.
        interview_path (Path): The path to the interview.
        interview_type (InterviewType): The type of interview.
        interview_datetime (datetime): The date and time of the interview.
        subject_id (str): The subject ID.
        study_id (str): The study ID.
    """

    def __init__(
        self,
        interview_name: str,
        interview_path: Path,
        interview_type: InterviewType,
        interview_datetime: datetime,
        subject_id: str,
        study_id: str,
        interview_id: Optional[int] = None,
    ):
        self.interview_id = interview_id
        self.interview_name = interview_name
        self.interview_path = interview_path
        self.interview_type = interview_type
        self.interview_datetime = interview_datetime
        self.subject_id = subject_id
        self.study_id = study_id

    def __str__(self):
        return f"Interview({self.subject_id}, {self.study_id}, {self.interview_name})"

    def __repr__(self):
        return self.__str__()

    @staticmethod
    def init_table_query() -> str:
        """
        Return the SQL query to create the 'interviews' table.
        """
        sql_query = """
        CREATE TABLE IF NOT EXISTS interviews (
            interview_id SERIAL PRIMARY KEY,
            interview_name TEXT NOT NULL,
            interview_path TEXT NOT NULL UNIQUE,
            interview_type TEXT NOT NULL REFERENCES interview_types (interview_type),
            interview_date TIMESTAMP NOT NULL,
            subject_id TEXT NOT NULL,
            study_id TEXT NOT NULL,
            FOREIGN KEY (subject_id, study_id) REFERENCES subjects (subject_id, study_id)
        );
        """

        return sql_query

    @staticmethod
    def post_init_queries() -> List[str]:
        """
        Return the SQL queries to add the 'is_primary ' column and
        unique constraint to the 'interviews' table.
        """
        queries: List[str] = []

        primary_column = (
            "ALTER TABLE interviews ADD COLUMN is_primary boolean DEFAULT false;"
        )
        unique_column = """
        CREATE UNIQUE INDEX idx_unique_primary_interview
        ON interviews(interview_name)
        WHERE is_primary = true;
        """
        create_view = """
        CREATE VIEW primary_interviews AS
        SELECT interview_name
        FROM interviews
        WHERE is_primary = true;
        """

        queries.append(primary_column)
        queries.append(unique_column)
        queries.append(create_view)

        return queries

    @staticmethod
    def drop_table_query() -> str:
        """
        Return the SQL query to drop the 'interviews' table.
        """
        sql_query = """
        DROP TABLE IF EXISTS interviews;
        """

        return sql_query

    def to_sql(self):
        """
        Return the SQL query to insert the Interview object into the 'interviews' table.
        """
        i_name = db.santize_string(self.interview_name)
        i_path = db.santize_string(str(self.interview_path))
        i_type = db.santize_string(self.interview_type.value)
        i_date = db.santize_string(str(self.interview_datetime))
        s_id = db.santize_string(self.subject_id)
        st_id = db.santize_string(self.study_id)

        sql_query = f"""
        INSERT INTO interviews (interview_name, interview_path, interview_type, interview_date, subject_id, study_id)
        VALUES ('{i_name}', '{i_path}', '{i_type}', '{i_date}', '{s_id}', '{st_id}')
        ON CONFLICT (interview_path) DO NOTHING;
        """

        return sql_query

    @staticmethod
    def get_interview_name(config_file: Path, interview_file: Path) -> Optional[str]:
        """
        Returns the Interview name associated with the file

        Args:
            config_file (Path): The path to the config file.
            interview_file (Path): The path to the interview file.

        Returns:
            Optional[str]: The name of the interview.
        """

        santiized_interview_file = db.santize_string(str(interview_file))

        query = f"""
        SELECT interview_name
        FROM interview_files
        INNER JOIN interviews USING (interview_path)
        WHERE interview_file = '{santiized_interview_file}';
        """

        result = db.fetch_record(config_file=config_file, query=query)

        return result
