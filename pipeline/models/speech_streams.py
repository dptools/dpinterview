#!/usr/bin/env python
"""
SppechStreams Model
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

from pipeline.helpers import db, utils

console = utils.get_console()


class SpeechStream:
    """
    Represents a Speech Stream with a single speaker.

    A Speech Stream is an audio stream that contains speech from a single speaker.

    Attributes:
        as_path (Path): Path to the audio stream
        ss_speaker_label (str): The speaker in the audio stream
        ss_path (Path): Path to the speech stream
    """

    def __init__(
        self,
        as_path: Path,
        ss_speaker_label: str,
        ss_path: Path,
    ):
        self.as_path = as_path
        self.ss_speaker_label = ss_speaker_label
        self.ss_path = ss_path

    def __repr__(self) -> str:
        return f"SpeechStream({self.ss_path}, speaker={self.ss_speaker_label})"

    def __str__(self) -> str:
        return self.__repr__()

    @staticmethod
    def init_table_query() -> str:
        """
        Returns the SQL query to create the speech_streams table.

        Returns:
            str: The SQL query to create the speech_streams table.
        """
        sql_query = """
        CREATE TABLE IF NOT EXISTS speech_streams (
            as_path TEXT NOT NULL REFERENCES speech_separation (as_path),
            ss_speaker_label TEXT NOT NULL,
            ss_path TEXT NOT NULL UNIQUE,
            PRIMARY KEY (as_path, ss_speaker_label)
        )
        """

        return sql_query

    @staticmethod
    def drop_table_query() -> str:
        """
        Returns the SQL query to drop the speech_streams table.

        Returns:
            str: The SQL query to drop the speech_streams table.
        """
        sql_query = "DROP TABLE IF EXISTS speech_streams"

        return sql_query

    def to_sql(self) -> str:
        """
        Returns the SQL query to insert the SpeechStream into the database.

        Returns:
            str: The SQL query to insert the SpeechStream into the database.
        """
        sql_query = f"""
        INSERT INTO speech_streams (as_path, ss_speaker_label, ss_path)
        VALUES ('{self.as_path}', '{self.ss_speaker_label}', '{self.ss_path}')
        """

        return sql_query


if __name__ == "__main__":
    config_file = utils.get_config_file_path()

    console.log("Initializing 'speech_streams' table...")
    console.log(
        "[red]This will delete all existing data in the 'speech_streams' table!"
    )

    drop_queries = [SpeechStream.drop_table_query()]
    create_queries = [SpeechStream.init_table_query()]

    sql_queries = drop_queries + create_queries

    db.execute_queries(config_file=config_file, queries=sql_queries, show_commands=True)

    console.log("'speech_streams' table initialized.")
