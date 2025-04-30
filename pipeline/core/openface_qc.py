"""
Helper functions to perform quality control on OpenFace output.
"""

import logging
from pathlib import Path
from typing import List, Optional

import pandas as pd

from pipeline.helpers import db
from pipeline.models.openface_qc import OpenfaceQC

logger = logging.getLogger(__name__)


def get_file_to_process(config_file: Path, study_id: str) -> Optional[Path]:
    """
    Fetches a file to process from the database.

    - Fetches a file that has not been processed yet.
        - Must be processed by OpenFace.

    Args:
        config_file (Path): Path to the config file.
        study_id (str): Study ID.

    Returns:
        Optional[Path]: Path to the file to process.
    """
    sql_query = f"""
        SELECT of_processed_path
        FROM openface AS of
        LEFT JOIN video_streams vs USING (vs_path)
        LEFT JOIN (
            SELECT decrypted_files.destination_path, interview_files.interview_file_tags
            FROM interview_files JOIN decrypted_files
            ON interview_files.interview_file = decrypted_files.source_path
        ) AS if
        ON vs.video_path = if.destination_path
        WHERE of_processed_path NOT in (
            SELECT of_processed_path FROM openface_qc
        ) AND vs.video_path IN (
            SELECT destination_path FROM decrypted_files
            LEFT JOIN interview_files ON interview_files.interview_file = decrypted_files.source_path
            LEFT JOIN interview_parts USING (interview_path)
            LEFT JOIN interviews USING (interview_name)
            WHERE interviews.study_id = '{study_id}'
        )
        ORDER BY RANDOM()
        LIMIT 1;
    """

    of_processed_path = db.fetch_record(config_file=config_file, query=sql_query)

    if of_processed_path is None:
        return None

    return Path(of_processed_path)


def run_openface_qc(of_processed_path: Path) -> OpenfaceQC:
    """
    Function to perform quality control on OpenFace output.

    Args:
        of_processed_path (str): Path containing the OpenFace output.

    Returns:
        OpenfaceQC: Object containing the results of the quality control.
    """

    openface_csv_path = of_processed_path.glob("*.csv")

    csv_paths: List[Path] = []
    for path in openface_csv_path:
        csv_paths.append(Path(path))

    if len(csv_paths) == 0:
        logger.error(f"No CSV files found in {of_processed_path}")
        raise FileNotFoundError(f"No CSV files found in {of_processed_path}")
    if len(csv_paths) > 1:
        logger.error(f"Multiple CSV files found in {of_processed_path}")
        raise FileNotFoundError(f"Multiple CSV files found in {of_processed_path}")

    df = pd.read_csv(csv_paths[0], on_bad_lines="warn")

    faces_count = df["face_id"].nunique()
    frames_count = df.shape[0]

    sucessful_frames_count = df[df["success"] == 1].shape[0]
    sucessful_frames_percentage = sucessful_frames_count / frames_count * 100

    success_df = df[df["success"] == 1]
    successful_frames_confidence_mean = success_df["confidence"].mean()
    successful_frames_confidence_std = success_df["confidence"].std()
    successful_frames_confidence_median = success_df["confidence"].median()

    passed = True
    # Fail if less than 50% of frames are successful
    if sucessful_frames_percentage < 50:
        passed = False

    return OpenfaceQC(
        of_processed_path=of_processed_path,
        faces_count=faces_count,
        frames_count=frames_count,
        sucessful_frames_count=sucessful_frames_count,
        sucessful_frames_percentage=sucessful_frames_percentage,
        successful_frames_confidence_mean=successful_frames_confidence_mean,
        successful_frames_confidence_std=successful_frames_confidence_std,
        successful_frames_confidence_median=successful_frames_confidence_median,
        passed=passed,
    )


def log_openface_qc(config_file: Path, openface_qc_result: OpenfaceQC) -> None:
    """
    Logs the results of the OpenFace QC to the database.

    Args:
        config_file (Path): Path to the config file.
        openface_qc_result (OpenfaceQC): Object containing the results of the quality control.
    """
    query = openface_qc_result.to_sql()

    db.execute_queries(config_file=config_file, queries=[query], show_commands=True)
