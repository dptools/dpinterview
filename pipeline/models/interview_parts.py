"""
InterviewPart model
"""

from pathlib import Path
from datetime import datetime
from typing import Optional, List

from pipeline.helpers import db


class InterviewParts:
    """
    Represents an interview part.

    Attributes:
        interview_name (str): The name of the interview.
        interview_path (Path): The path to the interview.
        interview_day (int): The day of the interview. (day 1 is consent day)
        interview_part (int): The part of the interview.
        interview_datetime (datetime): The date and time of the interview.
        is_primary (bool): Whether the interview is the primary interview.
        is_duplicate (bool): Whether the interview is a duplicate.
    """

    def __init__(
        self,
        interview_name: str,
        interview_path: Path,
        interview_day: int,
        interview_part: int,
        interview_datetime: datetime,
        is_primary: bool = False,
        is_duplicate: bool = False,
    ):
        self.interview_name = interview_name
        self.interview_path = interview_path
        self.interview_day = interview_day
        self.interview_part = interview_part
        self.interview_datetime = interview_datetime
        self.is_primary = is_primary
        self.is_duplicate = is_duplicate

    def __str__(self):
        return f"InterviewParts({self.interview_name} - {self.interview_part} - {self.interview_path})"

    def __repr__(self):
        return self.__str__()

    @staticmethod
    def init_table_query() -> str:
        """
        Return the SQL query to create the 'interview_parts' table.
        """
        sql_query = """
        CREATE TABLE IF NOT EXISTS interview_parts (
            interview_path TEXT PRIMARY KEY,
            interview_name TEXT REFERENCES interviews(interview_name),
            interview_day INTEGER NOT NULL,
            interview_part INTEGER NOT NULL,
            interview_datetime TIMESTAMP NOT NULL,
            is_primary BOOLEAN DEFAULT FALSE,
            is_duplicate BOOLEAN DEFAULT FALSE
        );
        """

        return sql_query

    @staticmethod
    def drop_table_query() -> List[str]:
        """
        Return the SQL query to drop the 'interviews_parts' table.
        """
        drop_queries = [
            "DROP TABLE IF EXISTS interview_parts;",
        ]

        return drop_queries

    def to_sql(self):
        """
        Return the SQL query to insert the InterviewParts object into the 'interviews_parts' table.
        """
        i_name = db.santize_string(self.interview_name)
        i_path = db.santize_string(str(self.interview_path))
        i_date = db.santize_string(str(self.interview_datetime))

        sql_query = f"""
        INSERT INTO interview_parts (
            interview_path, interview_name, interview_day,
            interview_part, interview_datetime,
            is_primary, is_duplicate
        ) VALUES (
            '{i_path}', '{i_name}', {self.interview_day},
            {self.interview_part}, '{i_date}',
            {self.is_primary}, {self.is_duplicate}
        ) ON CONFLICT (interview_path) DO NOTHING;
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
