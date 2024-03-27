"""
InterviewFiles model
"""

from pathlib import Path

from pipeline.helpers import db


class InterviewFile:
    """
    Represents a file associated with an interview.

    Attributes:
        interview_path (Path): The path to the interview.
        interview_file (Path): The path to the file.
        tags (str): The tags associated with the file.
    """

    def __init__(self, interview_path: Path, interview_file: Path, tags: str):
        self.interview_path = interview_path
        self.interview_file = interview_file
        self.tags = tags

    def __str__(self):
        return f"InterviewFiles({self.interview_path}, {self.interview_file})"

    def __repr__(self):
        return self.__str__()

    @staticmethod
    def drop_table_query() -> str:
        """
        Return the SQL query to drop the 'interview_files' table.
        """
        sql_query = """
        DROP TABLE IF EXISTS interview_files;
        """

        return sql_query

    @staticmethod
    def init_table_query() -> str:
        """
        Return the SQL query to create the 'interview_files' table.
        """
        sql_query = """
        CREATE TABLE IF NOT EXISTS interview_files (
            interview_path TEXT NOT NULL REFERENCES interviews (interview_path),
            interview_file TEXT UNIQUE NOT NULL REFERENCES files (file_path),
            interview_file_tags TEXT,
            ignored BOOLEAN DEFAULT FALSE,
            PRIMARY KEY (interview_path, interview_file)
        );
        """

        return sql_query

    @staticmethod
    def ignore_file(interview_file: Path) -> str:
        """
        Return the SQL query to ignore the file in the 'interview_files' table.

        Args:
            interview_file (Path): The path to the file to ignore.

        Returns:
            str: SQL query to ignore the file
        """
        sql_query = f"""
        UPDATE interview_files
        SET ignored = TRUE
        WHERE interview_file = '{interview_file}';
        """

        return sql_query

    def to_sql(self):
        """
        Return the SQL query to insert the InterviewFiles object into the 'interview_files' table.
        """
        i_path = db.santize_string(str(self.interview_path))
        i_file = db.santize_string(str(self.interview_file))

        sql_query = f"""
        INSERT INTO interview_files (interview_path, interview_file,
            interview_file_tags)
        VALUES ('{i_path}', '{i_file}',
            '{self.tags}')
        ON CONFLICT (interview_path, interview_file) DO
            UPDATE SET interview_file_tags = '{self.tags}';
        """

        return sql_query
