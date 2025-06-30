#!/usr/bin/env python
"""
TranscribemePull Model
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


class TranscribemePull:
    """
    Represents a downloaded transcription file from Transcribeme.

    Attributes:
        transcription_destination_path (Path): Path for the (local) transcription file
        sftp_download_path (Path): Path for the SFTP download
        sftp_archive_path (Path): Path to the remote transcription file (archived)
        sftp_download_duration_s (int): Duration of the SFTP download in seconds
        sftp_download_timestamp (datetime): Timestamp of the SFTP download
        completed_audio_file_path (Path): Path to the completed audio file
    """

    def __init__(
        self,
        transcription_destination_path: Path,
        sftp_download_path: Path,
        sftp_archive_path: Path,
        sftp_download_duration_s: int,
        sftp_download_timestamp: datetime,
        completed_audio_file_path: Path,
    ):
        self.transcription_destination_path = transcription_destination_path
        self.sftp_download_path = sftp_download_path
        self.sftp_archive_path = sftp_archive_path
        self.sftp_download_duration_s = sftp_download_duration_s
        self.sftp_download_timestamp = sftp_download_timestamp
        self.completed_audio_file_path = completed_audio_file_path

    def __repr__(self) -> str:
        return f"TranscribemePull({self.transcription_destination_path})"

    def __str__(self):
        return self.__repr__()

    @staticmethod
    def init_table_query() -> str:
        """
        Returns the SQL query to create the TranscribemePull table.
        """
        sql_query = """
        CREATE TABLE IF NOT EXISTS transcribeme.transcribeme_pull (
            transcription_destination_path TEXT PRIMARY KEY,
            sftp_download_path TEXT NOT NULL,
            sftp_archive_path TEXT NOT NULL,
            sftp_download_duration_s INTEGER NOT NULL,
            sftp_download_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            completed_audio_file_path TEXT NOT NULL,
            FOREIGN KEY (transcription_destination_path)
                REFERENCES transcribeme.transcribeme_push (transcription_destination_path)
        );
        """
        return sql_query

    @staticmethod
    def drop_table_query() -> str:
        """
        Returns the SQL query to drop the TranscribemePull table.
        """
        sql_query = """
        DROP TABLE IF EXISTS transcribeme.transcribeme_pull;
        """
        return sql_query

    def to_sql(self) -> str:
        """
        Returns the SQL query to insert a new TranscribemePull record.
        """
        sanitized_transcription_destination_path = db.santize_string(
            self.transcription_destination_path
        )
        sanitized_sftp_download_path = db.santize_string(self.sftp_download_path)
        sanitized_sftp_archive_path = db.santize_string(self.sftp_archive_path)
        sanitized_completed_audio_file_path = db.santize_string(
            self.completed_audio_file_path
        )

        sql_query = f"""
        INSERT INTO transcribeme.transcribeme_pull (
            transcription_destination_path,
            sftp_download_path,
            sftp_archive_path,
            sftp_download_duration_s,
            sftp_download_timestamp,
            completed_audio_file_path
        ) VALUES (
            '{sanitized_transcription_destination_path}',
            '{sanitized_sftp_download_path}',
            '{sanitized_sftp_archive_path}',
            {self.sftp_download_duration_s},
            '{self.sftp_download_timestamp}',
            '{sanitized_completed_audio_file_path}'
        );
        """
        return sql_query


if __name__ == "__main__":
    config_file = utils.get_config_file_path()

    console.log("Initializing `transcribeme.transcribeme_pull` table")
    console.log(
        "[red]This will delete all existing data in the 'transcribeme.transcribeme_pull' table!"
    )

    drop_queries = [TranscribemePull.drop_table_query()]
    create_queries = [TranscribemePull.init_table_query()]

    sql_queries = drop_queries + create_queries

    db.execute_queries(config_file=config_file, queries=sql_queries, show_commands=True)

    console.log("'transcribeme.transcribeme_pull' table initialized.")
