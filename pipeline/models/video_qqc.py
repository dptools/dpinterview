#!/usr/bin/env python
"""
VideoQuickQc Model
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

from typing import Optional

from pipeline.helpers import db, utils

console = utils.get_console()


class VideoQuickQc:
    """
    Results from the video quick qc process.

    Attributes:
        video_path (Path): The path to the video.
        has_black_bars (bool): Whether or not the video has black bars.
        black_bar_height (Optional[int]): The height of the black bars.
        process_time (Optional[float]): The time it took to process the video.
    """

    def __init__(
        self,
        video_path: Path,
        has_black_bars: bool,
        black_bar_height: Optional[int] = None,
        process_time: Optional[float] = None,
    ):
        self.video_path = video_path
        self.has_black_bars = has_black_bars
        self.black_bar_height = black_bar_height
        self.process_time = process_time

    def __repr__(self):
        return f"VideoQuickQc({self.video_path}, {self.has_black_bars}, {self.black_bar_height})"

    def __str__(self):
        return self.__repr__()

    @staticmethod
    def init_table_query() -> str:
        """
        Return the SQL query to create the 'video_quick_qc' table.
        """
        sql_query = """
        CREATE TABLE IF NOT EXISTS video_quick_qc (
            video_path TEXT NOT NULL REFERENCES decrypted_files (destination_path),
            has_black_bars BOOLEAN NOT NULL,
            black_bar_height INTEGER,
            vqqc_process_time REAL NOT NULL,
            vqqc_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (video_path)
        );
        """

        return sql_query

    @staticmethod
    def drop_table_query() -> str:
        """
        Return the SQL query to drop the 'video_quick_qc' table.
        """
        sql_query = """
        DROP TABLE IF EXISTS video_quick_qc;
        """

        return sql_query

    def to_sql(self):
        """
        Return the SQL query to insert the VideoQuickQc object into the 'video_quick_qc' table.
        """
        if self.black_bar_height is None:
            _black_bar_height = "NULL"
        else:
            _black_bar_height = self.black_bar_height

        if self.process_time is None:
            _process_time = "0.0"
        else:
            _process_time = self.process_time

        sql_query = f"""
        INSERT INTO video_quick_qc (
            video_path,
            has_black_bars,
            black_bar_height,
            vqqc_process_time
        ) VALUES (
            '{self.video_path}',
            {self.has_black_bars},
            {_black_bar_height},
            {_process_time}
        );
        """

        return sql_query


if __name__ == "__main__":
    config_file = utils.get_config_file_path()

    console.log("Initializing 'video_quick_qc' table...")
    console.log(
        "[red]This will delete all existing data in the 'video_quick_qc' table!"
    )

    drop_queries = [VideoQuickQc.drop_table_query()]
    create_queries = [VideoQuickQc.init_table_query()]

    sql_queries = drop_queries + create_queries

    console.log("Executing queries...")
    db.execute_queries(config_file=config_file, queries=sql_queries, show_commands=True)

    console.log("[green]Done!")
