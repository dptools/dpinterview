"""
InterviewFiles model
"""

from pathlib import Path
from typing import List

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
            interview_path TEXT NOT NULL REFERENCES interview_parts (interview_path),
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

    @staticmethod
    def get_interview_files_with_tag(config_file: Path, interview_name: str, tag: str) -> List[Path]:
        """
        Get all audio files for a given interview.

        Args:
            config_file (Path): The path to the configuration file.
            interview_name (str): The name of the interview.
            tag (str): The tag to filter the audio files by.

        Returns:
            List[Path]: A list of all audio files for the given interview.
        """

        query = f"""
            SELECT interview_file
            FROM interview_files
            LEFT JOIN interview_parts USING (interview_path)
            WHERE interview_name = '{interview_name}'
                AND interview_parts.is_primary = TRUE
                AND interview_files.interview_file_tags LIKE '%%{tag}%%';
        """

        results_df = db.execute_sql(config_file=config_file, query=query)

        audio_file_paths_s = results_df["interview_file"].tolist()
        audio_file_paths = [Path(p) for p in audio_file_paths_s]

        return audio_file_paths
