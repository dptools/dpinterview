#!/usr/bin/env python
"""
FaceProcessingPipelineMetrics Model
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

from datetime import datetime

from pipeline.helpers import db, utils
from pipeline.models.interview_roles import InterviewRole

console = utils.get_console()


class FaceProcessingPipelineMetrics:
    """Represents metrics for the face processing pipeline."""

    def __init__(
        self,
        interview_name: str,
        ir_role: InterviewRole,
        fpm_landmark_time_s: int,
        fpm_fpose_time_s: int,
        fpm_au_time_s: int,
        fpm_execution_time_s: int,
        fpm_record_time_s: int,
        fpm_timestamp: datetime = datetime.now(),
    ):
        self.interview_name = interview_name
        self.ir_role = ir_role
        self.fpm_landmark_time_s = fpm_landmark_time_s
        self.fpm_fpose_time_s = fpm_fpose_time_s
        self.fpm_au_time_s = fpm_au_time_s
        self.fpm_execution_time_s = fpm_execution_time_s
        self.fpm_record_time_s = fpm_record_time_s
        self.fpm_timestamp = fpm_timestamp

    def __repr__(self):
        return f"FaceProcessingPipelineMetrics({self.interview_name}, {self.ir_role}, {self.fpm_landmark_time_s}, \
            {self.fpm_fpose_time_s}, {self.fpm_au_time_s}, {self.fpm_execution_time_s}, {self.fpm_record_time_s})"

    def __str__(self):
        return self.__repr__()

    @staticmethod
    def init_table_query() -> str:
        """
        Returns SQL query to create the `face_processing_pipeline_metrics` table.
        """
        sql_query = """
        CREATE TABLE IF NOT EXISTS face_processing_pipeline_metrics (
            interview_name TEXT NOT NULL,
            ir_role TEXT NOT NULL,
            fpm_landmark_time_s INT NOT NULL,
            fpm_fpose_time_s INT NOT NULL,
            fpm_au_time_s INT NOT NULL,
            fpm_execution_time_s INT NOT NULL,
            fpm_record_time_s INT NOT NULL,
            fpm_timestamp TIMESTAMP NOT NULL,
            PRIMARY KEY (interview_name, ir_role),
            FOREIGN KEY (ir_role) REFERENCES interview_roles (ir_role)
        );
        """

        return sql_query

    @staticmethod
    def drop_table_query() -> str:
        """
        Returns SQL query to drop the `face_processing_pipeline_metrics` table.
        """
        sql_query = "DROP TABLE IF EXISTS face_processing_pipeline_metrics;"

        return sql_query

    def to_sql(self) -> str:
        """
        Returns SQL query to insert the object into the `face_processing_pipeline_metrics` table.
        """
        sql_query = f"""
        INSERT INTO face_processing_pipeline_metrics (
            interview_name,
            ir_role,
            fpm_landmark_time_s,
            fpm_fpose_time_s,
            fpm_au_time_s,
            fpm_execution_time_s,
            fpm_record_time_s,
            fpm_timestamp
        ) VALUES (
            '{self.interview_name}',
            '{self.ir_role}',
            {self.fpm_landmark_time_s},
            {self.fpm_fpose_time_s},
            {self.fpm_au_time_s},
            {self.fpm_execution_time_s},
            {self.fpm_record_time_s},
            '{self.fpm_timestamp}'
        );
        """

        return sql_query


if __name__ == "__main__":
    config_file = utils.get_config_file_path()

    console.log("Initializing `face_processing_pipeline_metrics` table...")
    console.log(
        "[red]This will delete all existing data in the 'face_processing_pipeline_metrics' table![/red]"
    )

    drop_queries = [FaceProcessingPipelineMetrics.drop_table_query()]
    create_queries = [FaceProcessingPipelineMetrics.init_table_query()]

    sql_queries = drop_queries + create_queries

    db.execute_queries(config_file=config_file, queries=sql_queries)

    console.log("[green]Done!")
