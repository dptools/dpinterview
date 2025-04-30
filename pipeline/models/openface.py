#!/usr/bin/env python
"""
Openface Model
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


from typing import Optional
from datetime import datetime

from pipeline.helpers import utils, db
from pipeline.models.interview_roles import InterviewRole

console = utils.get_console()


class Openface:
    """
    Represents an Openface Run.

    Attributes:
        vs_path (Path): The path to the video stream.
        ir_role (InterviewRole): The role of the interviewee.
        video_path (Path): The path to the video.
        of_processed_path (Path): The path to the processed video.
        of_process_time (float): The time it took to process the video.
        of_timestamp (datetime): The timestamp of the Openface run.
    """

    def __init__(
        self,
        vs_path: Path,
        ir_role: InterviewRole,
        video_path: Path,
        of_processed_path: Path,
        of_process_time: Optional[float] = None,
        of_overlay_provess_time: Optional[float] = None,
    ):
        self.vs_path = vs_path
        self.ir_role = ir_role
        self.video_path = video_path
        self.of_processed_path = of_processed_path
        self.of_process_time: Optional[float] = of_process_time
        self.of_overlay_provess_time: Optional[float] = of_overlay_provess_time
        self.of_timestamp = datetime.now()

    def __repr__(self):
        return f"Openface({self.vs_path}, {self.ir_role}, {self.of_processed_path})"

    def __str__(self):
        return self.__repr__()

    @staticmethod
    def init_table_query() -> str:
        """
        Return the SQL query to create the 'openface' table.
        """
        sql_query = """
        CREATE TABLE IF NOT EXISTS openface (
            vs_path TEXT NOT NULL PRIMARY KEY REFERENCES video_streams (vs_path),
            ir_role TEXT NOT NULL,
            video_path TEXT NOT NULL,
            of_processed_path TEXT NOT NULL UNIQUE,
            of_process_time REAL,
            of_overlay_provess_time REAL,
            of_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (video_path, ir_role) REFERENCES video_streams (video_path, ir_role)
        );
        """
        return sql_query

    @staticmethod
    def drop_table_query() -> str:
        """
        Return the SQL query to drop the 'openface' table.
        """
        sql_query = """
        DROP TABLE IF EXISTS openface;
        """
        return sql_query

    @staticmethod
    def drop_row_query(of_processed_path: Path) -> str:
        """
        Return the SQL query to delete a row from the 'openface' table.

        Args:
            of_processed_path (Path): Path to the processed video

        Returns:
            str: SQL query to delete the row
        """
        sql_query = f"""
        DELETE FROM openface
        WHERE of_processed_path = '{of_processed_path}';
        """
        return sql_query

    @staticmethod
    def drop_row_query_v(video_path: Path) -> str:
        """
        Return the SQL query to delete a row from the 'openface' table.

        Args:
            video_path (Path): Path to the source video

        Returns:
            str: SQL query to delete the row
        """
        sql_query = f"""
        DELETE FROM openface
        WHERE video_path = '{video_path}';
        """
        return sql_query

    @staticmethod
    def drop_row_query_vs(vs_path: Path) -> str:
        """
        Return the SQL query to delete a row from the 'openface' table.

        Args:
            video_path (Path): Path to the video stream

        Returns:
            str: SQL query to delete the row
        """
        sql_query = f"""
        DELETE FROM openface
        WHERE vs_path = '{vs_path}';
        """
        return sql_query

    def to_sql(self) -> str:
        """
        Return the SQL query to insert the object into the 'openface' table.
        """

        if self.of_process_time is None:
            process_time = "NULL"
        else:
            process_time = str(self.of_process_time)

        if self.of_overlay_provess_time is None:
            overlay_process_time = "NULL"
        else:
            overlay_process_time = str(self.of_overlay_provess_time)

        sql_query = f"""
        INSERT INTO openface (
            vs_path,
            ir_role,
            video_path,
            of_processed_path,
            of_process_time,
            of_overlay_provess_time,
            of_timestamp)
        VALUES (
            '{self.vs_path}',
            '{self.ir_role.value}',
            '{self.video_path}',
            '{self.of_processed_path}',
            {process_time},
            {overlay_process_time},
            '{self.of_timestamp}'
        );
        """

        return sql_query


if __name__ == "__main__":
    config_file = utils.get_config_file_path()

    console.log("Initializing 'openface' table...")
    console.log("[red]This will delete all existing data in the 'openface' table!")

    drop_queries = [Openface.drop_table_query()]
    create_queries = [Openface.init_table_query()]

    sql_queries = drop_queries + create_queries

    db.execute_queries(config_file=config_file, queries=sql_queries, show_commands=True)

    console.log("'openface' table initialized.")
