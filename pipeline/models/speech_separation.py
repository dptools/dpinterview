#!/usr/bin/env python
"""
SpeechSeparation Model
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


class SpeechSeparation:
    """
    Represents a Speech Separation run.

    A Speech Separation run is a process that separates an audio stream into
    multiple audio streams based on the speakers in the audio.

    Attributes:
        ss_source_path (Path): Path to the source file
        ss_diariazation_rttm (Path): Path to the diarization RTTM file
        ss_identified_speakers_count (int): The number of speakers identified
        ss_process_time (Optional[float]): The time it took to process the audio stream
        ss_timestamp (Optional[str]): The timestamp of the speech separation
    """

    def __init__(
        self,
        as_path: Path,
        ss_diariazation_rttm_path: Path,
        ss_identified_speakers_count: int,
        ss_process_time: Optional[float] = None,
        ss_timestamp: Optional[datetime] = None,
    ):
        self.as_path = as_path
        self.ss_diariazation_rttm_path = ss_diariazation_rttm_path
        self.ss_identified_speakers_count = ss_identified_speakers_count
        self.ss_process_time = ss_process_time
        if ss_timestamp is None:
            self.ss_timestamp = datetime.now()
        else:
            self.ss_timestamp = ss_timestamp

    def __repr__(self) -> str:
        as_path = self.as_path
        speakers_count = self.ss_identified_speakers_count
        return f"SpeechSeparation({as_path}, speakers_count={speakers_count})"

    def __str__(self) -> str:
        return self.__repr__()

    @staticmethod
    def init_table_query() -> str:
        """
        Returns the SQL query to create the speech_separation table.
        """
        sql_query = """
        CREATE TABLE IF NOT EXISTS speech_separation (
            as_path TEXT NOT NULL REFERENCES audio_streams (as_path),
            ss_diariazation_rttm_path TEXT NOT NULL,
            ss_identified_speakers_count INTEGER NOT NULL,
            ss_process_time REAL,
            ss_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (as_path)
        );
        """
        return sql_query

    @staticmethod
    def drop_table_query() -> str:
        """
        Returns the SQL query to drop the speech_separation table.
        """
        sql_query = "DROP TABLE IF EXISTS speech_separation;"

        return sql_query

    def to_sql(self) -> str:
        """
        Returns the SQL query to insert the SpeechSeparation object into the database.
        """

        sql_query = f"""
        INSERT INTO speech_separation (
            as_path, ss_diariazation_rttm_path, ss_identified_speakers_count,
            ss_process_time, ss_timestamp
        ) VALUES (
            '{self.as_path}', '{self.ss_diariazation_rttm_path}', {self.ss_identified_speakers_count},
            {self.ss_process_time}, '{self.ss_timestamp}'
        );
        """

        return sql_query


if __name__ == "__main__":
    config_file = utils.get_config_file_path()

    console.log("Initializing 'speech_separation' table...")
    console.log(
        "[red]This will delete all existing data in the 'speech_separation' table!"
    )

    drop_queries = [SpeechSeparation.drop_table_query()]
    create_queries = [SpeechSeparation.init_table_query()]

    sql_queries = drop_queries + create_queries

    db.execute_queries(config_file=config_file, queries=sql_queries, show_commands=True)

    console.log("'speech_separation' table initialized.")
