#!/usr/bin/env python
"""
TranscribemePush Model
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

from pipeline.helpers import db, utils

console = utils.get_console()


class TranscribemePush:
    """
    Represent a TranscribemePush.

    A TranscribemePush is an audio file sent to Transcribeme for transcription.

    Attributes:
        transcription_source_path (Path): Path to the source file
        source_language (str): Language of the source audio file
        sftp_upload_path (Path): Path for the SFTP upload
        transcription_destination_path (Path): Path for the transcription file
        sftp_upload_duration_s (int): Duration of the SFTP upload in seconds
        sftp_upload_timestamp (datetime): Timestamp of the SFTP upload
    """

    def __init__(
        self,
        transcription_source_path: Path,
        source_language: str,
        sftp_upload_path: str,
        transcription_destination_path: Path,
        sftp_upload_duration_s: int,
        sftp_upload_timestamp: datetime,
    ):
        self.transcription_source_path = transcription_source_path
        self.source_language = source_language
        self.sftp_upload_path = sftp_upload_path
        self.transcription_destination_path = transcription_destination_path
        self.sftp_upload_duration_s = sftp_upload_duration_s
        self.sftp_upload_timestamp = sftp_upload_timestamp

    def __repr__(self):
        return f"TranscribemePush({self.transcription_source_path})"

    def __str__(self):
        return self.__repr__()

    @staticmethod
    def init_table_query() -> str:
        """
        Return the SQL query to create the TranscribemePush table.
        """
        sql_query = """
            CREATE TABLE IF NOT EXISTS transcribeme.transcribeme_push (
                transcription_source_path TEXT PRIMARY KEY,
                source_language TEXT NOT NULL,
                sftp_upload_path TEXT NOT NULL,
                transcription_destination_path TEXT NOT NULL UNIQUE,
                sftp_upload_duration_s INT NOT NULL,
                sftp_upload_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (transcription_source_path) REFERENCES transcribeme.wav_conversion (wc_destination_path)
            );
        """
        return sql_query

    @staticmethod
    def drop_table_query():
        """
        Return the SQL query to drop the TranscribemePush table.
        """
        sql_query = "DROP TABLE IF EXISTS transcribeme.transcribeme_push;"
        return sql_query

    def to_sql(self):
        """
        Return the SQL query to insert the TranscribemePush into the table.
        """
        sanitized_source_path = db.santize_string(self.transcription_source_path)
        sanitized_destination_path = db.santize_string(
            self.transcription_destination_path
        )
        sanitized_sftp_path = db.santize_string(self.sftp_upload_path)

        sql_query = f"""
        INSERT INTO transcribeme.transcribeme_push (
            transcription_source_path,
            source_language,
            sftp_upload_path,
            transcription_destination_path,
            sftp_upload_duration_s,
            sftp_upload_timestamp
        )
        VALUES (
            '{sanitized_source_path}',
            '{self.source_language}',
            '{sanitized_sftp_path}',
            '{sanitized_destination_path}',
            {self.sftp_upload_duration_s},
            '{self.sftp_upload_timestamp}'
        )
        """

        return sql_query


if __name__ == "__main__":
    config_file = utils.get_config_file_path()

    console.log("Initializing `transcribeme.transcribeme_push` table")
    console.log(
        "[red]This will delete all existing data in the 'transcribeme.transcribeme_push' table!"
    )

    create_schema_query = "CREATE SCHEMA IF NOT EXISTS transcribeme;"
    drop_queries = [TranscribemePush.drop_table_query()]
    create_queries = [create_schema_query, TranscribemePush.init_table_query()]

    sql_queries = drop_queries + create_queries

    db.execute_queries(config_file=config_file, queries=sql_queries, show_commands=True)

    console.log("'transcribeme.transcribeme_push' table initialized.")
