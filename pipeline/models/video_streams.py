#!/usr/bin/env python
"""
VideoStream Model
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
from pipeline.models.interview_roles import InterviewRole

console = utils.get_console()


class VideoStream:
    """
    Represent a VideoStream.

    A VideoStream is a video with a role in an interview.
    This VideoStream is processed by OpenFace.

    Attributes:
        video_path (Path): The path to the video.
        ir_role (InterviewRole): The role of the interviewee.
        vs_path (Path): The path to the video stream.
        vs_process_time (Optional[float]): The time it took to process the video stream.
    """

    def __init__(
        self,
        video_path: Path,
        ir_role: InterviewRole,
        vs_path: Path,
        vs_process_time: Optional[float] = None,
    ):
        self.video_path = video_path
        self.ir_role = ir_role
        self.vs_path = vs_path
        self.vs_process_time = vs_process_time

    def __repr__(self):
        return f"VideoStream({self.video_path}, {self.ir_role}, {self.vs_path})"

    def __str__(self):
        return self.__repr__()

    @staticmethod
    def init_table_query() -> str:
        """
        Return the SQL query to create the 'video_streams' table.
        """
        sql_query = """
        CREATE TABLE IF NOT EXISTS video_streams (
            vs_path TEXT NOT NULL PRIMARY KEY,
            video_path TEXT NOT NULL REFERENCES video_quick_qc (video_path),
            ir_role TEXT NOT NULL REFERENCES interview_roles (ir_role),
            vs_process_time REAL,
            vs_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE (video_path, ir_role)
        );
        """

        return sql_query

    @staticmethod
    def drop_table_query() -> str:
        """
        Return the SQL query to drop the 'video_streams' table.
        """
        sql_query = """
        DROP TABLE IF EXISTS video_streams;
        """

        return sql_query

    @staticmethod
    def drop_row_query(stream_path: Path) -> str:
        """
        Return the SQL query to delete a row from the 'video_streams' table.

        Args:
            stream_path (Path): Path to the video stream

        Returns:
            str: SQL query to delete the row
        """
        sql_query = f"""
        DELETE FROM video_streams
        WHERE vs_path = '{stream_path}';
        """

        return sql_query

    def to_sql(self) -> str:
        """
        Return the SQL query to insert this object into the 'video_streams' table.
        """

        if self.vs_process_time is None:
            process_time = "NULL"
        else:
            process_time = str(self.vs_process_time)

        sql_query = f"""
        INSERT INTO video_streams (video_path, ir_role, vs_path, vs_process_time)
        VALUES ('{self.video_path}', '{self.ir_role.value}', '{self.vs_path}', {process_time});
        """

        return sql_query


if __name__ == "__main__":
    config_file = utils.get_config_file_path()

    console.log("Initializing 'video_streams' table...")
    console.log("[red]This will delete all existing data in the 'video_streams' table!")

    drop_queries = [VideoStream.drop_table_query()]
    create_queries = [VideoStream.init_table_query()]

    sql_queries = drop_queries + create_queries

    db.execute_queries(config_file=config_file, queries=sql_queries, show_commands=True)

    console.log("'video_streams' table initialized.")
