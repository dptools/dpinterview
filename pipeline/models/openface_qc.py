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
from datetime import datetime

import pandas as pd

from pipeline.helpers import utils, db

console = utils.get_console()


class OpenfaceQC:
    def __init__(
        self,
        of_processed_path: Path,
        faces_count: int,
        frames_count: int,
        sucessful_frames_count: int,
        sucessful_frames_percentage: float,
        successful_frames_confidence_mean: float,
        successful_frames_confidence_std: float,
        successful_frames_confidence_median: float,
        passed: bool,
        ofqc_process_time: Optional[float] = None,
    ):
        self.of_processed_path = of_processed_path
        self.faces_count = faces_count
        self.frames_count = frames_count
        self.sucessful_frames_count = sucessful_frames_count
        self.sucessful_frames_percentage = sucessful_frames_percentage
        self.successful_frames_confidence_mean = successful_frames_confidence_mean
        self.successful_frames_confidence_std = successful_frames_confidence_std
        self.successful_frames_confidence_median = successful_frames_confidence_median
        self.passed = passed
        self.ofqc_process_time: Optional[float] = ofqc_process_time
        self.ofqc_timestamp = datetime.now()

    def __repr__(self):
        return f"OpenfaceQC({self.of_processed_path}, {self.passed})"

    def __str__(self):
        return self.__repr__()

    @staticmethod
    def init_table_query() -> str:
        """
        Return the SQL query to create the 'openface' table.
        """
        sql_query = """
        CREATE TABLE IF NOT EXISTS openface_qc (
            of_processed_path TEXT NOT NULL PRIMARY KEY REFERENCES openface (of_processed_path),
            faces_count INTEGER NOT NULL,
            frames_count INTEGER NOT NULL,
            sucessful_frames_count INTEGER NOT NULL,
            sucessful_frames_percentage REAL NOT NULL,
            successful_frames_confidence_mean REAL,
            successful_frames_confidence_std REAL,
            successful_frames_confidence_median REAL,
            passed BOOLEAN NOT NULL,
            ofqc_process_time REAL,
            ofqc_timestamp TIMESTAMP NOT NULL
        );
        """
        return sql_query

    @staticmethod
    def drop_table_query() -> str:
        """
        Return the SQL query to drop the 'openface' table.
        """
        sql_query = """
        DROP TABLE IF EXISTS openface_qc;
        """
        return sql_query

    def to_sql(self) -> str:
        """
        Return the SQL query to insert the object into the 'openface' table.
        """

        if self.ofqc_process_time is None:
            process_time = "NULL"
        else:
            process_time = str(self.ofqc_process_time)

        if pd.isna(self.successful_frames_confidence_mean):
            successful_frames_confidence_mean = "NULL"
        else:
            successful_frames_confidence_mean = self.successful_frames_confidence_mean
        if pd.isna(self.successful_frames_confidence_std):
            successful_frames_confidence_std = "NULL"
        else:
            successful_frames_confidence_std = self.successful_frames_confidence_std
        if pd.isna(self.successful_frames_confidence_median):
            successful_frames_confidence_median = "NULL"
        else:
            successful_frames_confidence_median = (
                self.successful_frames_confidence_median
            )

        sql_query = f"""
        INSERT INTO openface_qc (
            of_processed_path,
            faces_count,
            frames_count,
            sucessful_frames_count,
            sucessful_frames_percentage,
            successful_frames_confidence_mean,
            successful_frames_confidence_std,
            successful_frames_confidence_median,
            passed,
            ofqc_process_time,
            ofqc_timestamp
        ) VALUES (
            '{self.of_processed_path}',
            {self.faces_count},
            {self.frames_count},
            {self.sucessful_frames_count},
            {self.sucessful_frames_percentage},
            {successful_frames_confidence_mean},
            {successful_frames_confidence_std},
            {successful_frames_confidence_median},
            {self.passed},
            {process_time},
            '{self.ofqc_timestamp}'
        );
        """

        return sql_query


if __name__ == "__main__":
    config_file = utils.get_config_file_path()

    console.log("Initializing 'openface_qc' table...")
    console.log("[red]This will delete all existing data in the 'openface_qc' table!")

    drop_queries = [OpenfaceQC.drop_table_query()]
    create_queries = [OpenfaceQC.init_table_query()]

    sql_queries = drop_queries + create_queries

    db.execute_queries(config_file=config_file, queries=sql_queries, show_commands=True)

    console.log("'openface_qc' table initialized.")
