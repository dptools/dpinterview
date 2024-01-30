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

from pipeline.helpers import utils, db

console = utils.get_console()


class LoadOpenface:
    def __init__(
        self,
        interview_name: str,
        subject_id: str,
        study_id: str,
        subject_of_processed_path: str,
        interviewer_of_processed_path: Optional[str] = None,
        lof_notes: Optional[str] = None,
        lof_report_generation_possible: bool = False,
        lof_process_time: Optional[float] = None,
    ):
        self.interview_name = interview_name
        self.subject_id = subject_id
        self.study_id = study_id
        self.subject_of_processed_path = subject_of_processed_path
        self.interviewer_of_processed_path = interviewer_of_processed_path
        self.lof_notes = lof_notes
        self.lof_process_time: Optional[float] = lof_process_time
        self.lof_report_generation_possible = lof_report_generation_possible
        self.lof_timestamp = datetime.now()

    def __repr__(self):
        return f"LoadOpenface({self.interview_name}, {self.subject_id}, {self.subject_of_processed_path}, \
            {self.interviewer_of_processed_path}, {self.lof_notes}, {self.lof_process_time}, \
            {self.lof_report_generation_possible}, {self.lof_timestamp})"

    def __str__(self):
        return self.__repr__()

    @staticmethod
    def init_table_query() -> str:
        """
        Return the SQL query to create the 'openface' table.
        """
        sql_query = """
        CREATE TABLE IF NOT EXISTS load_openface (
            interview_name TEXT NOT NULL,
            subject_id TEXT NOT NULL,
            study_id TEXT NOT NULL,
            subject_of_processed_path TEXT REFERENCES openface (of_processed_path),
            interviewer_of_processed_path TEXT,
            lof_notes TEXT,
            lof_process_time REAL,
            lof_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            lof_report_generation_possible BOOLEAN NOT NULL DEFAULT FALSE,
            PRIMARY KEY (interview_name),
            FOREIGN KEY (subject_id, study_id) REFERENCES subjects (subject_id, study_id)
        );
        """
        return sql_query

    @staticmethod
    def drop_table_query() -> str:
        """
        Return the SQL query to drop the 'openface' table.
        """
        sql_query = """
        DROP TABLE IF EXISTS load_openface;
        """
        return sql_query

    def to_sql(self) -> str:
        """
        Return the SQL query to insert the object into the 'load_openface' table.
        """

        if self.lof_process_time is None:
            process_time = "NULL"
        else:
            process_time = str(self.lof_process_time)

        if self.interviewer_of_processed_path is None:
            interviewer_of_processed_path = "NULL"
        else:
            interviewer_of_processed_path = db.santize_string(
                self.interviewer_of_processed_path
            )

        if self.lof_notes is None:
            notes = "NULL"
        else:
            notes = db.santize_string(self.lof_notes)

        sql_query = f"""
        INSERT INTO load_openface (interview_name, subject_id, study_id, \
            subject_of_processed_path, interviewer_of_processed_path, lof_notes, \
            lof_process_time, lof_report_generation_possible, lof_timestamp) \
        VALUES ('{self.interview_name}', '{self.subject_id}', '{self.study_id}', \
            '{self.subject_of_processed_path}','{interviewer_of_processed_path}', '{notes}', \
            {process_time}, {self.lof_report_generation_possible}, '{self.lof_timestamp}'
        );
        """

        sql_query = db.handle_null(sql_query)

        return sql_query


if __name__ == "__main__":
    config_file = utils.get_config_file_path()

    console.log("Initializing 'load_openface' table...")
    console.log("[red]This will delete all existing data in the 'load_openface' table!")

    drop_queries = [LoadOpenface.drop_table_query()]
    create_queries = [LoadOpenface.init_table_query()]

    sql_queries = drop_queries + create_queries

    db.execute_queries(config_file=config_file, queries=sql_queries, show_commands=True)

    console.log("'load_openface' table initialized.")
