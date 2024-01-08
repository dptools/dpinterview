#!/usr/bin/env python

import sys
from pathlib import Path

file = Path(__file__).resolve()
parent = file.parent
root = None
for parent in file.parents:
    if parent.name == "av-pipeline-v2":
        root = parent
sys.path.append(str(root))

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
            video_path TEXT NOT NULL REFERENCES video_quick_qc (video_path),
            ir_role TEXT NOT NULL REFERENCES interview_roles (ir_role),
            vs_path TEXT NOT NULL UNIQUE,
            vs_process_time REAL,
            vs_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (video_path, ir_role)
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

    def to_sql(self) -> str:
        """
        Return the SQL query to insert this object into the 'video_streams' table.
        """
        sql_query = f"""
        INSERT INTO video_streams (video_path, ir_role, vs_path, vs_process_time)
        VALUES ('{self.video_path}', '{self.ir_role.value}', '{self.vs_path}', {self.vs_process_time});
        """

        return sql_query


if __name__ == "__main__":
    config_file = utils.get_config_file_path()

    console.log("Initializing 'video_streams' table...")
    console.log("[red]This will delete all existing data in the 'video_streams' table!")

    drop_queries = [VideoStream.drop_table_query()]
    create_queries = [VideoStream.init_table_query()]

    sql_queries = drop_queries + create_queries

    sql_query = VideoStream.init_table_query()
    db.execute_queries(config_file=config_file, queries=sql_queries, show_commands=True)

    console.log("'video_streams' table initialized.")
