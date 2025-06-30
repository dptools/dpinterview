#!/usr/bin/env python
"""
AudioQC Model
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
from typing import Dict, List, Optional

from pipeline.helpers import db, utils

console = utils.get_console()


class AudioQC:
    """
    Represent a quality control check for audio files.

    Attributes:
        aqc_source_path (Path): The source path of the audio file.
        aqc_passed (bool): Whether the audio file passed the QC checks.
        aqc_metrics (Dict[str, float]): Metrics collected during the QC checks.
        aqc_fail_reasons Dict[str, bool]: Reasons for failure if the QC did not pass.
        aqc_duration_s (int): Duration of the audio file in seconds.
        aqc_timestamp (datetime): The timestamp when the QC was performed.
    """

    def __init__(
        self,
        aqc_source_path: Path,
        aqc_passed: bool,
        aqc_metrics: Dict[str, float],
        aqc_fail_reasons: Dict[str, bool],
        aqc_duration_s: float,
        aqc_timestamp: Optional[datetime] = None,
    ):
        """
        Initialize the AudioQC object.
        """
        self.aqc_source_path = aqc_source_path
        self.aqc_passed = aqc_passed
        self.aqc_metrics = aqc_metrics
        self.aqc_fail_reasons = aqc_fail_reasons
        self.aqc_duration_s = aqc_duration_s
        self.aqc_timestamp = aqc_timestamp or datetime.now()

    def __repr__(self) -> str:
        return (
            f"AudioQC(aqc_source_path={self.aqc_source_path}, "
            f"aqc_passed={self.aqc_passed})"
        )

    def __str__(self):
        return self.__repr__()

    @staticmethod
    def init_table_query() -> List[str]:
        """
        Return the SQL query to create the transcribeme.audio_qc table.
        """
        sql_query = """
            CREATE TABLE IF NOT EXISTS transcribeme.audio_qc (
                aqc_source_path TEXT NOT NULL PRIMARY KEY REFERENCES transcribeme.wav_conversion(wc_destination_path),
                aqc_passed BOOLEAN NOT NULL,
                aqc_metrics JSONB NOT NULL,
                aqc_fail_reasons JSONB NULL,
                aqc_duration_s FLOAT NOT NULL,
                aqc_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                aqc_override BOOLEAN DEFAULT FALSE
            );
        """

        index_query = """
            CREATE INDEX IF NOT EXISTS idx_audio_qc_source_path_passed
            ON transcribeme.audio_qc (aqc_source_path, aqc_passed);
        """
        return [sql_query, index_query]

    @staticmethod
    def drop_table_query() -> str:
        """
        Return the SQL query to drop the transcribeme.audio_qc table.
        """
        sql_query = """
            DROP TABLE IF EXISTS transcribeme.audio_qc;
        """
        return sql_query

    def to_sql(self) -> str:
        """
        Convert the AudioQC object to an SQL insert statement.
        """
        metrics_str = db.sanitize_json(self.aqc_metrics)

        if self.aqc_fail_reasons is None:
            self.aqc_fail_reasons = {}

        fail_reasons_str = db.sanitize_json(self.aqc_fail_reasons)

        sql_query = f"""
            INSERT INTO transcribeme.audio_qc (
                aqc_source_path,
                aqc_passed,
                aqc_metrics,
                aqc_fail_reasons,
                aqc_duration_s,
                aqc_timestamp
            ) VALUES (
                '{self.aqc_source_path}',
                {self.aqc_passed},
                '{metrics_str}',
                '{fail_reasons_str}',
                {self.aqc_duration_s},
                '{self.aqc_timestamp}'
            );
        """

        sql_query = db.handle_null(sql_query)
        return sql_query


if __name__ == "__main__":
    config_file = utils.get_config_file_path()

    console.log("Initializing `transcribeme.audio_qc` table")
    console.log(
        "[red]This will delete all existing data in the 'transcribeme.audio_qc' table!"
    )

    drop_queries = [AudioQC.drop_table_query()]
    create_queries = AudioQC.init_table_query()

    sql_queries = drop_queries + create_queries

    db.execute_queries(config_file=config_file, queries=sql_queries, show_commands=True)

    console.log("'transcribeme.audio_qc' table initialized.")
