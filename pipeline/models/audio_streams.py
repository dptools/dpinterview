#!/usr/bin/env python
"""
AudioStream Model
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

from typing import Optional

from pipeline.helpers import db, utils
from pipeline.models.interview_roles import InterviewRole

console = utils.get_console()


class AudioStream:
    """
    Represent a AudioStream.

    A AudioStream is an audio stream extracted extracted from a video file or
    a stream of audio data.

    Attributes:
        as_source_path (Path): Path to the source file
        as_source_index (int): Index of the audio stream in the source file
        as_path (Path): Path to the audio stream
        ir_role (Optional[InterviewRole]): The role of the person in the audio stream
        as_process_time (Optional[float]): The time it took to process the audio stream
    """

    def __init__(
        self,
        as_source_path: Path,
        as_source_index: int,
        as_path: Path,
        ir_role: Optional[InterviewRole] = None,
        as_process_time: Optional[float] = None,
        as_notes: Optional[str] = None,
    ):
        self.as_source_path = as_source_path
        self.as_source_index = as_source_index
        self.as_path = as_path
        self.ir_role = ir_role
        self.as_process_time = as_process_time
        self.as_notes = as_notes

    def __repr__(self):
        return f"AudioStream({self.as_source_path}, {self.as_source_index}, {self.as_path}, {self.ir_role})"

    def __str__(self):
        return self.__repr__()

    @staticmethod
    def init_table_query() -> str:
        """
        Return the SQL query to create the 'audio_streams' table.
        """
        sql_query = """
        CREATE TABLE IF NOT EXISTS audio_streams (
            as_source_path TEXT NOT NULL REFERENCES decrypted_files (destination_path),
            as_source_index INTEGER NOT NULL,
            as_path TEXT NOT NULL UNIQUE,
            ir_role TEXT,
            as_notes TEXT,
            as_process_time REAL,
            as_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (as_source_path, as_source_index)
        );
        """

        return sql_query

    @staticmethod
    def drop_table_query() -> str:
        """
        Return the SQL query to drop the 'audio_streams' table.
        """
        sql_query = """
        DROP TABLE IF EXISTS audio_streams;
        """

        return sql_query

    @staticmethod
    def drop_row_query_s(stream_path: Path) -> str:
        """
        Return the SQL query to delete a row from the 'audio_streams' table.

        Args:
            stream_path (Path): Path to the video stream

        Returns:
            str: SQL query to delete the row
        """
        sql_query = f"""
        DELETE FROM audio_streams
        WHERE as_path = '{stream_path}';
        """

        return sql_query

    @staticmethod
    def drop_row_query_v(source_path: Path) -> str:
        """
        Return the SQL query to delete a row from the 'audio_streams' table.

        Args:
            source_path (Path): Path to the source file

        Returns:
            str: SQL query to delete the row
        """
        sql_query = f"""
        DELETE FROM audio_streams
        WHERE as_source_path = '{source_path}';
        """

        return sql_query

    def to_sql(self) -> str:
        """
        Return the SQL query to insert this object into the 'audio_streams' table.
        """

        if self.ir_role:
            role = self.ir_role.value
        else:
            role = "NULL"

        if self.as_process_time is None:
            process_time = "NULL"
        else:
            process_time = self.as_process_time

        if self.as_notes is None:
            notes = "NULL"
        else:
            notes = f"{self.as_notes}"

        sql_query = f"""
        INSERT INTO audio_streams (as_source_path, as_source_index, as_path, ir_role, as_process_time, as_notes)
        VALUES ('{self.as_source_path}', {self.as_source_index}, '{self.as_path}', '{role}', {process_time}, '{notes}');
        """

        sql_query = db.handle_null(sql_query)

        return sql_query


if __name__ == "__main__":
    config_file = utils.get_config_file_path()

    console.log("Initializing 'audio_streams' table...")
    console.log("[red]This will delete all existing data in the 'audio_streams' table!")

    drop_queries = [AudioStream.drop_table_query()]
    create_queries = [AudioStream.init_table_query()]

    sql_queries = drop_queries + create_queries

    db.execute_queries(config_file=config_file, queries=sql_queries, show_commands=True)

    console.log("'audio_streams' table initialized.")
