#!/usr/bin/env python
"""
RedactedTranscript Model
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


class RedactedTranscript:
    """
    Represents a redacted transcription file.

    Attributes:
        unredacted_transcript_path (Path): Path for the unredacted transcription file
        redacted_transcript_path (Path): Path for the redacted transcription file
        redaction_timestamp (datetime): Timestamp of when the redaction was performed
    """

    def __init__(
        self,
        unredacted_transcript_path: Path,
        redacted_transcript_path: Path,
        redaction_timestamp: datetime,
    ):
        self.unredacted_transcript_path = unredacted_transcript_path
        self.redacted_transcript_path = redacted_transcript_path
        self.redaction_timestamp = redaction_timestamp

    def __repr__(self) -> str:
        return f"RedactedTranscript({self.redacted_transcript_path})"

    @staticmethod
    def init_table_query() -> str:
        """
        Returns the SQL query to initialize the RedactedTranscript table.
        """
        sql_query = """
        CREATE TABLE transcribeme.redacted_transcripts (
            unredacted_transcript_path TEXT PRIMARY KEY
                REFERENCES transcribeme.transcribeme_pull(transcription_destination_path),
            redacted_transcript_path TEXT NOT NULL,
            redaction_timestamp TIMESTAMP NOT NULL
        );
        """
        return sql_query

    @staticmethod
    def drop_table_query() -> str:
        """
        Returns the SQL query to drop the RedactedTranscript table.
        """
        sql_query = "DROP TABLE IF EXISTS transcribeme.redacted_transcripts;"
        return sql_query

    def to_sql(self) -> str:
        """
        Returns the SQL insert statement for the RedactedTranscript instance.
        """
        sql_query = f"""
        INSERT INTO transcribeme.redacted_transcripts (
            unredacted_transcript_path,
            redacted_transcript_path,
            redaction_timestamp
        ) VALUES (
            '{self.unredacted_transcript_path}',
            '{self.redacted_transcript_path}',
            '{self.redaction_timestamp}'
        );
        """
        return sql_query


if __name__ == "__main__":
    config_file = utils.get_config_file_path()

    console.log("Initializing `transcribeme.redacted_transcripts` table")
    console.log(
        "[red]This will delete all existing data in the 'transcribeme.redacted_transcripts' table!"
    )

    drop_queries = [RedactedTranscript.drop_table_query()]
    create_queries = [RedactedTranscript.init_table_query()]

    sql_queries = drop_queries + create_queries

    db.execute_queries(config_file=config_file, queries=sql_queries, show_commands=True)

    console.log("'transcribeme.redacted_transcripts' table initialized.")
