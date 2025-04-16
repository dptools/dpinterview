"""
Interview model
"""

from pathlib import Path
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
        interview_name (str): The name of the interview.
        interview_path (Path): The path to the interview.
        interview_type (InterviewType): The type of interview.
        interview_day (int): The day of the interview. (day 1 is consent day)
        interview_part (int): The part of the interview.
        interview_datetime (datetime): The date and time of the interview.
        subject_id (str): The subject ID.
        study_id (str): The study ID.
    """

    def __init__(
        self,
        interview_name: str,
        interview_type: InterviewType,
        subject_id: str,
        study_id: str,
    ):
        self.interview_name = interview_name
        self.interview_type = interview_type
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
            interview_name TEXT NOT NULL PRIMARY KEY,
            interview_type TEXT NOT NULL REFERENCES interview_types (interview_type),
            subject_id TEXT NOT NULL,
            study_id TEXT NOT NULL,
            FOREIGN KEY (subject_id, study_id) REFERENCES subjects (subject_id, study_id)
        );
        """

        return sql_query

    @staticmethod
    def drop_table_query() -> List[str]:
        """
        Return the SQL query to drop the 'interviews' table.
        """
        drop_queries = [
            "DROP TABLE IF EXISTS interviews;"
        ]

        return drop_queries

    def to_sql(self):
        """
        Return the SQL query to insert the Interview object into the 'interviews' table.
        """
        i_name = db.santize_string(self.interview_name)
        i_type = db.santize_string(self.interview_type.value)
        s_id = db.santize_string(self.subject_id)
        st_id = db.santize_string(self.study_id)

        sql_query = f"""
        INSERT INTO interviews (
            interview_name, interview_type,
            subject_id, study_id
        )
        VALUES (
            '{i_name}', '{i_type}',
            '{s_id}', '{st_id}'
        )
        ON CONFLICT (interview_name) DO NOTHING;
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
        LEFT JOIN interview_parts USING (interview_path)
        WHERE interview_file = '{santiized_interview_file}';
        """

        result = db.fetch_record(config_file=config_file, query=query)

        return result
