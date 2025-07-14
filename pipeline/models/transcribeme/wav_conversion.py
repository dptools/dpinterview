#!/usr/bin/env python
"""
WAV Conversion Model
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
from typing import List

from pipeline.helpers import db, utils

console = utils.get_console()


class WavConversion:
    """
    Represent a WAV Conversion.

    Attributes:
        wc_source_path (Path): Path to the source file
        wc_destination_path (Path): Path for the converted WAV file
        wc_duration_s (int): Duration of the WAV file in seconds
        wc_timestamp (datetime): Timestamp of the WAV conversion
    """

    def __init__(
        self,
        wc_source_path: Path,
        wc_destination_path: Path,
        wc_duration_s: int,
    ):
        self.wc_source_path = wc_source_path
        self.wc_destination_path = wc_destination_path
        self.wc_duration_s = wc_duration_s
        self.wc_timestamp = datetime.now()

    def __repr__(self) -> str:
        return f"WavConversion({self.wc_destination_path})"

    def __str__(self):
        return self.__repr__()

    @staticmethod
    def init_table_query() -> List[str]:
        """
        Return the SQL query to create the wav_conversion table.
        """
        sql_query = """
            CREATE TABLE IF NOT EXISTS transcribeme.wav_conversion (
                wc_source_path TEXT NOT NULL PRIMARY KEY,
                wc_destination_path TEXT NOT NULL UNIQUE,
                wc_duration_s INT NOT NULL,
                wc_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (wc_source_path) REFERENCES files (file_path)
                    ON DELETE SET NULL
                    ON UPDATE CASCADE
            );
        """
        return [sql_query]

    @staticmethod
    def drop_table_query() -> List[str]:
        """
        Return the SQL query to drop the wav_conversion table.
        """
        sql_query = """
            DROP TABLE IF EXISTS transcribeme.wav_conversion;
        """
        return [sql_query]

    def to_sql(self) -> str:
        """
        Convert the WavConversion object to an SQL insert statement.
        """
        sanitized_wc_source_path = db.santize_string(str(self.wc_source_path))
        sanitized_wc_destination_path = db.santize_string(str(self.wc_destination_path))

        sql_query = f"""
            INSERT INTO transcribeme.wav_conversion (
                wc_source_path,
                wc_destination_path,
                wc_duration_s,
                wc_timestamp
            ) VALUES (
                '{sanitized_wc_source_path}',
                '{sanitized_wc_destination_path}',
                {self.wc_duration_s},
                '{self.wc_timestamp}'
            )
            ON CONFLICT (wc_source_path) DO UPDATE SET
                wc_destination_path = EXCLUDED.wc_destination_path,
                wc_duration_s = EXCLUDED.wc_duration_s,
                wc_timestamp = EXCLUDED.wc_timestamp;
        """
        return sql_query


if __name__ == "__main__":
    config_file = utils.get_config_file_path()

    console.log("Initializing `transcribeme.wav_conversion` table")
    console.log(
        "[red]This will delete all existing data in the 'transcribeme.wav_conversion' table!"
    )

    drop_queries = [WavConversion.drop_table_query()]
    create_queries = [WavConversion.init_table_query()]

    sql_queries = drop_queries + create_queries

    db.execute_queries(config_file=config_file, queries=sql_queries, show_commands=True)

    console.log("'transcribeme.wav_conversion' table initialized.")
