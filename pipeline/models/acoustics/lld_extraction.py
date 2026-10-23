#!/usr/bin/env python
"""
LldExtraction Model
"""

import sys
from pathlib import Path

file = Path(__file__).resolve()
parent = file.parent
ROOT = None
for parent in file.parents:
    if parent.name == 'dpinterview':
        ROOT = parent
sys.path.append(str(ROOT))

# remove current directory from path
try:
    sys.path.remove(str(parent))
except ValueError:
    pass

from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Literal

from pipeline.helpers import db, utils

console = utils.get_console()

SCHEMA_NAME = 'acoustics'
TABLE_NAME = f'{SCHEMA_NAME}.lld_extraction'

SourceKind = Literal['interview', 'journal']


@dataclass
class LldExtraction:
    """Acoustic features extracted from one source recording.

    Attributes:
        lld_source_path: The original recording (`files.file_path`); stable across WAV moves.
        lld_source_kind: Whether the recording is an interview or an audio journal.
        lld_audio_path: Path the audio was actually read from.
        lld_output_path: Path of the per-frame LLD CSV.
        lld_summary_path: Path of the functionals JSON, if one was written.
        lld_num_frames: Number of rows (10 ms frames) in the CSV.
        lld_duration_s: Wall-clock extraction time in seconds.
        lld_timestamp: When the extraction finished.
    """

    lld_source_path: Path
    lld_source_kind: SourceKind
    lld_audio_path: Path
    lld_output_path: Path
    lld_summary_path: Path | None
    lld_num_frames: int
    lld_duration_s: float
    lld_timestamp: datetime = field(default_factory=datetime.now)

    @staticmethod
    def init_table_query() -> List[str]:
        """
        Return the SQL queries to create the acoustics schema and table.
        """
        schema_query = f'CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME};'

        table_query = f"""
            CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                lld_source_path TEXT NOT NULL PRIMARY KEY REFERENCES files (file_path),
                lld_source_kind TEXT NOT NULL CHECK (lld_source_kind IN ('interview', 'journal')),
                lld_audio_path TEXT NOT NULL,
                lld_output_path TEXT NOT NULL UNIQUE,
                lld_summary_path TEXT UNIQUE,
                lld_num_frames INTEGER NOT NULL,
                lld_duration_s FLOAT NOT NULL,
                lld_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
        """
        return [schema_query, table_query]

    @staticmethod
    def drop_table_query() -> str:
        """
        Return the SQL query to drop the table, leaving the schema in place.
        """
        return f'DROP TABLE IF EXISTS {TABLE_NAME};'

    def to_sql(self) -> str:
        """
        Return the SQL query to insert this record.
        """
        source_path = db.santize_string(self.lld_source_path)
        audio_path = db.santize_string(self.lld_audio_path)
        output_path = db.santize_string(self.lld_output_path)
        summary_path = (
            'NULL' if self.lld_summary_path is None
            else db.santize_string(self.lld_summary_path)
        )

        sql_query = f"""
            INSERT INTO {TABLE_NAME} (
                lld_source_path,
                lld_source_kind,
                lld_audio_path,
                lld_output_path,
                lld_summary_path,
                lld_num_frames,
                lld_duration_s,
                lld_timestamp
            ) VALUES (
                '{source_path}',
                '{self.lld_source_kind}',
                '{audio_path}',
                '{output_path}',
                '{summary_path}',
                {self.lld_num_frames},
                {self.lld_duration_s},
                '{self.lld_timestamp}'
            );
        """
        return db.handle_null(sql_query)


if __name__ == '__main__':
    config_file = utils.get_config_file_path()

    # Create-only: dropping would discard the record of an already-extracted backlog.
    console.log(f'Initializing `{TABLE_NAME}` table (existing rows are kept)')

    db.execute_queries(
        config_file=config_file,
        queries=LldExtraction.init_table_query(),
        show_commands=True,
    )

    console.log(f"'{TABLE_NAME}' table initialized.")
