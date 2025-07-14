#!/usr/bin/env python
"""
FacepipeQc Model
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
from typing import Dict, Any

import pandas as pd

from pipeline.helpers import db, utils

console = utils.get_console()


class FacepipeQc:
    """
    Represents a FacepipeQc run.
    """

    def __init__(
        self, fp_source_video_path: Path, interview_name: str, qc_file_path: Path
    ) -> None:
        qc_df = pd.read_csv(qc_file_path)

        # Change 1st col name to interview_name
        qc_df.rename(columns={qc_df.columns[0]: "interview_name"}, inplace=True)
        qc_df["interview_name"] = interview_name

        self.fp_source_video_path: Path = fp_source_video_path
        self.interview_name: str = qc_df["interview_name"].iloc[0]

        # Cols:
        # rate_miss_faces,rate_miss_landm,avg_face_confid,avg_nfaces,
        # rate_face_low_cnr,rate_img_overexp,avg_facebox_width,
        # avg_facebox_height,median_pitch,median_yaw,median_roll

        self.rate_miss_faces: float = qc_df["rate_miss_faces"].iloc[0]
        self.rate_miss_landm: float = qc_df["rate_miss_landm"].iloc[0]
        self.avg_face_confid: float = qc_df["avg_face_confid"].iloc[0]
        self.avg_nfaces: float = qc_df["avg_nfaces"].iloc[0]
        self.rate_face_low_cnr: float = qc_df["rate_face_low_cnr"].iloc[0]
        self.rate_img_overexp: float = qc_df["rate_img_overexp"].iloc[0]
        self.avg_facebox_width: float = qc_df["avg_facebox_width"].iloc[0]
        self.avg_facebox_height: float = qc_df["avg_facebox_height"].iloc[0]
        self.median_pitch: float = qc_df["median_pitch"].iloc[0]
        self.median_yaw: float = qc_df["median_yaw"].iloc[0]
        self.median_roll: float = qc_df["median_roll"].iloc[0]

        self.other_fields: Dict[str, Any] = {
            col: qc_df[col].iloc[0] for col in qc_df.columns if col not in self.__dict__
        }

        self.fpqc_timestamp: datetime = datetime.now()

    def __repr__(self) -> str:
        return f"FacepipeQc({self.fp_source_video_path}, {self.interview_name})"

    def __str__(self):
        return self.__repr__()

    @staticmethod
    def init_table_query() -> str:
        """
        Returns the SQL query to initialize the FacepipeQc table.
        """
        init_table_query: str = """
        CREATE TABLE IF NOT EXISTS facepipe.facepipe_qc (
            fp_source_video_path TEXT REFERENCES
                facepipe.facepipe_runs(fp_source_video_path)
                ON DELETE CASCADE NOT NULL PRIMARY KEY,
            interview_name TEXT NOT NULL,
            rate_miss_faces FLOAT NOT NULL,
            rate_miss_landm FLOAT NOT NULL,
            avg_face_confid FLOAT NOT NULL,
            avg_nfaces FLOAT NOT NULL,
            rate_face_low_cnr FLOAT NOT NULL,
            rate_img_overexp FLOAT NOT NULL,
            avg_facebox_width FLOAT NOT NULL,
            avg_facebox_height FLOAT NOT NULL,
            median_pitch FLOAT NOT NULL,
            median_yaw FLOAT NOT NULL,
            median_roll FLOAT NOT NULL,
            other_fields JSONB NULL,
            fpqc_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        """
        return init_table_query

    @staticmethod
    def drop_table_query() -> str:
        """
        Returns the SQL query to drop the FacepipeQc table.
        """
        drop_table_query: str = """
        DROP TABLE IF EXISTS facepipe.facepipe_qc;
        """
        return drop_table_query

    def to_sql(self) -> str:
        """
        Returns the SQL query to insert this FacepipeQc instance into the database.
        """

        other_fields_sanitized = db.sanitize_json(self.other_fields)

        sql_query: str = f"""
        INSERT INTO facepipe.facepipe_qc (
            fp_source_video_path,
            interview_name,
            rate_miss_faces,
            rate_miss_landm,
            avg_face_confid,
            avg_nfaces,
            rate_face_low_cnr,
            rate_img_overexp,
            avg_facebox_width,
            avg_facebox_height,
            median_pitch,
            median_yaw,
            median_roll,
            other_fields,
            fpqc_timestamp
        ) VALUES (
            '{self.fp_source_video_path}',
            '{self.interview_name}',
            {self.rate_miss_faces},
            {self.rate_miss_landm},
            {self.avg_face_confid},
            {self.avg_nfaces},
            {self.rate_face_low_cnr},
            {self.rate_img_overexp},
            {self.avg_facebox_width},
            {self.avg_facebox_height},
            {self.median_pitch},
            {self.median_yaw},
            {self.median_roll},
            '{other_fields_sanitized}',
            '{self.fpqc_timestamp}'
        );
        """
        return sql_query


if __name__ == "__main__":
    config_file = utils.get_config_file_path()

    console.log("Initializing `facepipe.facepipe_qc` table")
    console.log(
        "[red]This will delete all existing data in the 'facepipe.facepipe_qc' table!"
    )

    drop_queries = [FacepipeQc.drop_table_query()]
    create_queries = [FacepipeQc.init_table_query()]

    sql_queries = drop_queries + create_queries

    db.execute_queries(config_file=config_file, queries=sql_queries, show_commands=True)

    console.log("'facepipe.facepipe_qc' table initialized.")
