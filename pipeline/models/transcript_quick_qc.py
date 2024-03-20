#!/usr/bin/env python
"""
TranscriptQuickQc Model
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

from typing import Optional, Dict, Any
from datetime import datetime

from pipeline.helpers import db, utils

console = utils.get_console()


class TranscriptQuickQc:
    """
    Results from the transcript quick qc process.

    Attributes:
        transcript_path (Path): The path to the transcript.
        speaker_metrics (dict): The speaker metrics.
        process_time (Optional[float]): The time it took to process the transcript.
    """

    def __init__(
        self,
        transcript_path: Path,
        speaker_metrics: Dict[str, Dict[str, Any]],
        process_time: Optional[float] = None,
        timestamp: datetime = datetime.now()
    ):
        self.transcript_path = transcript_path
        self.speaker_metrics = speaker_metrics
        self.process_time = process_time
        self.timestamp = timestamp

    def __repr__(self):
        return f"TranscriptQuickQc({self.transcript_path}, {self.speaker_metrics})"

    def __str__(self):
        return self.__repr__()

    @staticmethod
    def init_table_query() -> str:
        """
        Returns the query to initialize the table.
        """
        sql_query = """
        CREATE TABLE IF NOT EXISTS transcript_quick_qc (
            transcript_path TEXT NOT NULL REFERENCES files (file_path),
            speaker_metrics JSONB NOT NULL,
            process_time FLOAT,
            tqc_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (transcript_path)
        )
        """

        return sql_query

    @staticmethod
    def drop_table_query() -> str:
        """
        Returns the query to drop the table.
        """
        sql_query = "DROP TABLE IF EXISTS transcript_quick_qc"
        return sql_query

    def insert_query(self) -> str:
        """
        Returns the query to insert the object into the table.

        Returns:
            str: The SQL insert query.
        """
        speaker_metrics = db.sanitize_json(self.speaker_metrics)

        if self.process_time is None:
            process_time = "NULL"
        else:
            process_time = self.process_time

        sql_query = f"""
        INSERT INTO transcript_quick_qc (transcript_path, speaker_metrics, process_time, tqc_timestamp)
        VALUES ('{self.transcript_path}', '{speaker_metrics}', {process_time}, '{self.timestamp}')
        """

        sql_query = db.handle_null(sql_query)

        return sql_query


if __name__ == "__main__":
    config_file = utils.get_config_file_path()

    console.log("Initializing 'transcript_quick_qc' table...")
    console.log(
        "[red]This will delete all existing data in the 'transcript_quick_qc' table!"
    )

    drop_queries = [TranscriptQuickQc.drop_table_query()]
    create_queries = [TranscriptQuickQc.init_table_query()]

    sql_queries = drop_queries + create_queries

    console.log("Executing queries...")
    db.execute_queries(config_file=config_file, queries=sql_queries, show_commands=True)

    console.log("[green]Done!")
