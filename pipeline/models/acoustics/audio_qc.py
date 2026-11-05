#!/usr/bin/env python
"""
AudioQc Model
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
from typing import List

from pipeline.helpers import db, utils

console = utils.get_console()

SCHEMA_NAME = 'acoustics'
TABLE_NAME = f'{SCHEMA_NAME}.audio_qc'


@dataclass
class AudioQc:
    """Hard-failure QC verdict on one source recording, gating acoustic extraction.

    Attributes:
        qc_source_path: The original recording (`files.file_path`).
        qc_passed: Whether every check passed.
        qc_fail_reasons: The checks that failed; empty if passed.
        qc_audio_duration_s: Decoded signal length in seconds; None if undecodable.
        qc_peak_amplitude: Largest absolute sample; None if undecodable.
        qc_timestamp: When the check ran.
    """

    qc_source_path: Path
    qc_passed: bool
    qc_fail_reasons: tuple[str, ...]
    qc_audio_duration_s: float | None
    qc_peak_amplitude: float | None
    qc_timestamp: datetime = field(default_factory=datetime.now)

    @staticmethod
    def init_table_query() -> List[str]:
        """
        Return the SQL queries to create the acoustics schema and table.
        """
        schema_query = f'CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME};'

        table_query = f"""
            CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                qc_source_path TEXT NOT NULL PRIMARY KEY REFERENCES files (file_path),
                qc_passed BOOLEAN NOT NULL,
                qc_fail_reasons TEXT[] NOT NULL DEFAULT '{{}}',
                qc_audio_duration_s FLOAT,
                qc_peak_amplitude FLOAT,
                qc_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
        """
        return [schema_query, table_query]

    @staticmethod
    def drop_table_query() -> str:
        """
        Return the SQL query to drop the table, and `lld_extraction`'s foreign key to it.
        """
        return f'DROP TABLE IF EXISTS {TABLE_NAME} CASCADE;'

    def to_sql(self) -> str:
        """
        Return the SQL query to insert this record.
        """
        source_path = db.santize_string(self.qc_source_path)
        fail_reasons = ', '.join(
            f"'{db.santize_string(reason)}'" for reason in self.qc_fail_reasons
        )
        duration = 'NULL' if self.qc_audio_duration_s is None else self.qc_audio_duration_s
        peak = 'NULL' if self.qc_peak_amplitude is None else self.qc_peak_amplitude

        return f"""
            INSERT INTO {TABLE_NAME} (
                qc_source_path,
                qc_passed,
                qc_fail_reasons,
                qc_audio_duration_s,
                qc_peak_amplitude,
                qc_timestamp
            ) VALUES (
                '{source_path}',
                {self.qc_passed},
                ARRAY[{fail_reasons}]::TEXT[],
                {duration},
                {peak},
                '{self.qc_timestamp}'
            );
        """


if __name__ == '__main__':
    config_file = utils.get_config_file_path()

    # Create-only: dropping would discard every verdict, re-queueing the whole backlog.
    console.log(f'Initializing `{TABLE_NAME}` table (existing rows are kept)')

    db.execute_queries(
        config_file=config_file,
        queries=AudioQc.init_table_query(),
        show_commands=True,
    )

    console.log(f"'{TABLE_NAME}' table initialized.")
