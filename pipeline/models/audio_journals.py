#!/usr/bin/env python
"""
AudioJournal Model
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


class AudioJournal:
    """
    Represent a AudioJournal.

    A AudioStream is an audio stream extracted from Mindlamp, and is usally
    a short audio clip.

    Attributes:
        aj_path (Path): Path to the source file
        aj_name (str): Name of the audio journal
        aj_datetime (datetime): Date of the audio journal
        aj_day (int): Day of the audio journal (1 is the first day / consent day)
        aj_session (int): Session of the audio journal, for multiple audio journals in a day
        subject_id (str): The subject ID of the person in the audio journal
        study_id (str): The study ID of the person in the audio journal
    """

    def __init__(
        self,
        aj_path: Path,
        aj_name: str,
        aj_datetime: datetime,
        aj_day: int,
        aj_session: int,
        subject_id: str,
        study_id: str,
    ):
        self.aj_path = aj_path
        self.aj_name = aj_name
        self.aj_datetime = aj_datetime
        self.aj_day = aj_day
        self.aj_session = aj_session
        self.subject_id = subject_id
        self.study_id = study_id

    def __repr__(self):
        return f"AudioJournal({self.subject_id}, {self.aj_day}, {self.aj_session})"

    def __str__(self):
        return self.__repr__()

    @staticmethod
    def init_table_query() -> str:
        """
        Return the query to create the AudioJournal table.
        """
        sql_query = """
        CREATE TABLE IF NOT EXISTS audio_journals (
            aj_path TEXT NOT NULL PRIMARY KEY REFERENCES files (file_path),
            aj_name TEXT NOT NULL,
            aj_datetime TIMESTAMP NOT NULL,
            aj_day INTEGER NOT NULL,
            aj_session INTEGER NOT NULL,
            subject_id TEXT NOT NULL,
            study_id TEXT NOT NULL,
            FOREIGN KEY (subject_id, study_id) REFERENCES subjects (subject_id, study_id),
            UNIQUE (aj_day, aj_session, subject_id, study_id)
        )
        """

        return sql_query

    @staticmethod
    def drop_table_query() -> str:
        """
        Return the query to drop the AudioJournal table.
        """
        sql_query = """
        DROP TABLE IF EXISTS audio_journals
        """

        return sql_query

    def to_sql(self) -> str:
        """
        Return the SQL query to insert the AudioJournal into the table.
        """

        sanitized_path = db.santize_string(str(self.aj_path))

        sql_query = f"""
        INSERT INTO audio_journals (aj_path, aj_name, aj_datetime, aj_day,
            aj_session, subject_id, study_id)
        VALUES ('{sanitized_path}', '{self.aj_name}', '{self.aj_datetime}', {self.aj_day},
            {self.aj_session}, '{self.subject_id}', '{self.study_id}')
        ON CONFLICT (aj_path) DO UPDATE SET
            aj_name = excluded.aj_name,
            aj_datetime = excluded.aj_datetime,
            aj_day = excluded.aj_day,
            aj_session = excluded.aj_session,
            subject_id = excluded.subject_id,
            study_id = excluded.study_id;
        """

        return sql_query


if __name__ == "__main__":
    config_file = utils.get_config_file_path()

    console.log("Initializing `audio_journals` table")
    console.log(
        "[red]This will delete all existing data in the 'audio_journals' table!"
    )

    drop_queries = [AudioJournal.drop_table_query()]
    create_queries = [AudioJournal.init_table_query()]

    sql_queries = drop_queries + create_queries

    db.execute_queries(config_file=config_file, queries=sql_queries, show_commands=True)

    console.log("'audio_journals' table initialized.")
