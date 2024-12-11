#!/usr/bin/env python
"""
Transcription Model
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

from pipeline.helpers import db, utils

console = utils.get_console()


class Transcription:
    """
    Represents a Whisper Transcription Run.

    Attributes:
        t_audio_path (Path): Path to the audio file.
        t_transcript_json_path (Path): Path to the transcript json
        t_model (str): Model used for transcription
        t_process_time (float): Time taken to transcribe in seconds
        t_timestamp (datetime): Timestamp of transcription
    """

    def __init__(
        self,
        t_audio_path: Path,
        t_transcript_json_path: Path,
        t_model: str,
        t_process_time: float,
        t_timestamp: Optional[datetime] = None,
    ):
        self.t_audio_path = t_audio_path
        self.t_transcript_json_path = t_transcript_json_path
        self.t_model = t_model
        self.t_process_time = t_process_time
        self.t_timestamp = t_timestamp or datetime.now()

    def __repr__(self) -> str:
        return f"Transcription({self.t_transcript_json_path} {self.t_model})"

    def __str__(self) -> str:
        return self.__repr__()

    @staticmethod
    def init_table_query() -> str:
        """
        Returns the query to create the transcription table
        """
        sql_query = """
        CREATE TABLE transcription (
            t_audio_path TEXT REFERENCES speech_streams (ss_path),
            t_transcript_json_path TEXT NOT NULL UNIQUE,
            t_model TEXT NOT NULL,
            t_process_time FLOAT NOT NULL,
            t_timestamp TIMESTAMP NOT NULL,
            PRIMARY KEY (t_audio_path, t_model)
        )
        """

        return sql_query

    @staticmethod
    def drop_table_query() -> str:
        """
        Returns the query to drop the transcription table
        """
        sql_query = """
        DROP TABLE IF EXISTS transcription
        """

        return sql_query

    def to_sql(self) -> str:
        """
        Returns the query to insert the transcription into the database
        """
        sql_query = f"""
        INSERT INTO transcription (
            t_audio_path, t_transcript_json_path,
            t_model, t_process_time, t_timestamp
        ) VALUES (
            '{self.t_audio_path}', '{self.t_transcript_json_path}',
            '{self.t_model}', {self.t_process_time}, '{self.t_timestamp}'
        )
        """

        return sql_query


if __name__ == "__main__":
    config_file = utils.get_config_file_path()

    console.log("Initializing 'transcription' table...")
    console.log("[red]This will delete all existing data in the 'transcription' table!")

    drop_queries = [Transcription.drop_table_query()]
    create_queries = [Transcription.init_table_query()]

    sql_queries = drop_queries + create_queries

    db.execute_queries(config_file=config_file, queries=sql_queries, show_commands=True)

    console.log("'transcription' table initialized.")
