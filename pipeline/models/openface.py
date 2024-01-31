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
    if parent.name == "av-pipeline-v2":
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
    ):
        self.vs_path = vs_path
        self.ir_role = ir_role
        self.video_path = video_path
        self.of_processed_path = of_processed_path
        self.of_process_time: Optional[float] = of_process_time
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

    def to_sql(self) -> str:
        """
        Return the SQL query to insert the object into the 'openface' table.
        """

        if self.of_process_time is None:
            process_time = "NULL"
        else:
            process_time = str(self.of_process_time)

        sql_query = f"""
        INSERT INTO openface (vs_path, ir_role, video_path, of_processed_path, of_process_time)
        VALUES (
            '{self.vs_path}',
            '{self.ir_role.value}',
            '{self.video_path}',
            '{self.of_processed_path}',
            {process_time}
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
