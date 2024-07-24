"""
Module contain helper functions specific to this data pipeline
"""

import logging
from datetime import datetime
from functools import lru_cache
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

import pandas as pd

from pipeline import constants
from pipeline.helpers import db, dpdash, utils
from pipeline.models.interview_roles import InterviewRole

logger = logging.getLogger(__name__)


def get_consent_date_from_subject_id(
    config_file: Path, subject_id: str, study_id
) -> Optional[str]:
    """
    Retrieves the consent date for a given subject ID and study ID.

    Args:
        config_file (Path): The path to the configuration file.
        subject_id (str): The ID of the subject.
        study_id: The ID of the study.

    Returns:
        Optional[str]: The consent date if found, None otherwise.
    """
    query = f"""
        SELECT consent_date
        FROM subjects
        WHERE subject_id = '{subject_id}' AND study_id = '{study_id}';
    """

    results = db.fetch_record(config_file=config_file, query=query)

    if results is None:
        return None
    else:
        return results


def get_subject_ids(config_file: Path, study_id: str) -> List[str]:
    """
    Gets the subject IDs from the database.

    Args:
        config_file (Path): The path to the configuration file.
        study_id (str): The study ID.

    Returns:
        List[str]: A list of subject IDs.
    """
    query = f"""
        SELECT subject_id
        FROM subjects
        WHERE study_id = '{study_id}'
        ORDER BY subject_id;
    """

    results = db.execute_sql(config_file=config_file, query=query)

    subject_ids = results["subject_id"].tolist()

    return subject_ids


def get_all_cols(csv_file: Path) -> List[str]:
    """
    Returns a list of all column names in a CSV file.

    Args:
        csv_file (str): The path to the CSV file.

    Returns:
        List[str]: A list of all column names in the CSV file.
    """
    with open(csv_file, "r", encoding="utf-8") as f:
        cols = f.readline().strip().split(",")
    return cols


def get_openface_datatypes(config_file: Path, csv_file: Path) -> Dict[str, str]:
    """
    Assigns a SQL datatype to each column in the csv file.

    Args:
        config_file (str): The path to the configuration file.
        csv_file (str): The path to the CSV file.

    Returns:
        Dict[str, str]: A dictionary mapping each column name to its corresponding SQL datatype.
    """
    cols = get_all_cols(csv_file)

    params = utils.config(path=config_file, section="openface_features")

    int_cols = params["int_cols"].split(",")
    bool_cols = params["bool_cols"].split(",")
    time_cols = params["time_cols"].split(",")

    # rest of the columns are floats

    datatypes = {}

    for col in cols:
        if col in int_cols:
            datatypes[col] = "INTEGER"
        elif col in bool_cols:
            datatypes[col] = "BOOLEAN"
        elif col in time_cols:
            datatypes[col] = "TIME"
        else:
            datatypes[col] = "FLOAT"

    return datatypes


def get_openface_path(
    config_file: Path,
    interview_name: str,
    role: InterviewRole,
) -> Optional[Path]:
    """
    Get the path to the openface directory for the given interview, subject, and role.

    Args:
        config_file (Path): The path to the configuration file.
        interview_name (str): The name of the interview.
        subject_id (str): The ID of the subject.
        study_id (str): The ID of the study.
        role (InterviewRole): The role of the user.

    Returns:
        Path: The path to the openface directory.
    """

    dpdash_dict = dpdash.parse_dpdash_name(interview_name)
    subject_id = dpdash_dict["subject"]
    study_id = dpdash_dict["study"]

    query = f"""
    SELECT subject_of_processed_path, interviewer_of_processed_path
    FROM load_openface
    WHERE interview_name = '{interview_name}'
        AND subject_id = '{subject_id}'
        AND study_id = '{study_id}'
    """

    results = db.execute_sql(config_file=config_file, query=query)

    if results.empty:
        logger.error(
            f"No openface path found for interview {interview_name}, \
subject {subject_id}, study {study_id}: Probably not loaded into the database yet"
        )
        raise ValueError(
            f"No openface path found for interview {interview_name}, \
subject {subject_id}, study {study_id}: Probably not loaded into the database yet"
        )

    if role == InterviewRole.SUBJECT:
        openface_path = results.iloc[0]["subject_of_processed_path"]
    elif role == InterviewRole.INTERVIEWER:
        openface_path = results.iloc[0]["interviewer_of_processed_path"]
    else:
        raise ValueError(f"Invalid interview role: {role}")

    try:
        of_path = Path(openface_path)
    except TypeError:
        return None

    if not of_path.exists():
        raise FileNotFoundError(f"OpenFace path {of_path} does not exist")

    return of_path


def get_openfece_features_overlaid_video_path(
    config_file: Path,
    interview_name: str,
    role: InterviewRole,
) -> Optional[Path]:
    """
    Get the path to the openface directory for the given interview, subject, and role.

    Args:
        config_file (Path): The path to the configuration file.
        interview_name (str): The name of the interview.
        subject_id (str): The ID of the subject.
        study_id (str): The

    Returns:
        Path: The path to the openface directory.
    """

    of_path = get_openface_path(
        config_file=config_file,
        interview_name=interview_name,
        role=role,
    )

    if of_path is None:
        return None

    overlaid_video_path = of_path / "openface_aligned.mp4"

    if not overlaid_video_path.exists():
        return None

    return overlaid_video_path


# def construct_frame_paths(
#     frame_numbers: List[Optional[int]],
#     interview_name: str,
#     role: InterviewRole,
#     config_file: Path,
# ) -> List[Optional[Path]]:
#     """
#     Gets the paths to the frames for the given frame numbers.

#     Args:
#         frame_numbers (List[Optional[int]]): The frame numbers.
#         interview_name (str): The name of the interview.
#         subject_id (str): The ID of the subject.
#         study_id (str): The ID of the study.
#         role (InterviewRole): The role of the user.
#         config_file (Path): The path to the configuration file.

#     Returns:
#         List[Optional[Path]]: The paths to the frames.
#     """
#     frame_paths: List[Optional[Path]] = []

#     openface_path = get_openface_path(
#         config_file=config_file,
#         interview_name=interview_name,
#         role=role,
#     )

#     if openface_path is None:
#         raise FileNotFoundError(
#             f"No openface path found for interview {interview_name}"
#         )

#     frames_path = next(openface_path.glob("*aligned"))

#     for frame_number in frame_numbers:
#         if frame_number is None:
#             frame_paths.append(None)
#         else:
#             # Sample name: frame_det_00_000001.bmp
#             frame_file = f"frame_det_00_{frame_number:06d}.bmp"
#             frame_path = frames_path / frame_file

#             # Check if file exists
#             if not frame_path.exists():
#                 frame_paths.append(None)
#             else:
#                 frame_paths.append(frame_path)

#     return frame_paths


def get_interview_visit_count(config_file: Path, interview_name: str) -> Optional[int]:
    """
    Get the visit count for the given interview.

    Args:
        config_file (Path): The path to the configuration file.
        interview_name (str): The name of the interview.

    Returns:
        Optional[int]: The visit count if found, None otherwise.
    """

    dpdash_dict = dpdash.parse_dpdash_name(interview_name)

    subject_id = dpdash_dict["subject"]

    sql_query = f"""
        SELECT buffer.interview_count FROM (
            SELECT interview_name, interview_date,
                DENSE_RANK() OVER(ORDER BY interview_date ASC) AS interview_count
            FROM interviews AS osi
            WHERE subject_id = '{subject_id}'
        ) AS buffer
        WHERE buffer.interview_name = '{interview_name}';
    """

    visit_count = db.fetch_record(config_file=config_file, query=sql_query)

    if visit_count is None:
        return None

    return int(visit_count)


def get_total_visits_for_subject(config_file: Path, subject_id: str) -> int:
    """
    Get the total number of visits for a subject.

    Args:
        config_file (Path): The path to the configuration file.
        subject_id (str): The ID of the subject.

    Returns:
        int: The total number of visits for the subject.
    """
    query = f"""
    SELECT COUNT(DISTINCT interview_name) AS total_visits
    FROM interviews
    WHERE subject_id = '{subject_id}';
    """

    results = db.fetch_record(config_file=config_file, query=query)

    if results is None:
        return 0

    return int(results)


def get_interview_datetime(config_file: Path, interview_name: str) -> datetime:
    """
    Get the datetime of the interview.

    Args:
        config_file (Path): The path to the configuration file.
        interview_name (str): The name of the interview.

    Returns:
        datetime: The datetime of the interview.
    """
    query = f"""
    SELECT interview_date
    FROM interviews
    WHERE interview_name = '{interview_name}';
    """

    results = db.fetch_record(config_file=config_file, query=query)

    if results is None:
        raise ValueError(f"No interview date found for interview {interview_name}")

    interview_datetime = datetime.strptime(results, "%Y-%m-%d %H:%M:%S")

    return interview_datetime


def get_interview_type(config_file: Path, interview_name: str) -> str:
    """
    Get the type of the interview.

    Args:
        config_file (Path): The path to the configuration file.
        interview_name (str): The name of the interview.

    Returns:
        str: The type of the interview.
    """
    query = f"""
    SELECT interview_type
    FROM interviews
    WHERE interview_name = '{interview_name}';
    """

    results = db.fetch_record(config_file=config_file, query=query)

    if results is None:
        raise ValueError(f"No interview type found for interview {interview_name}")

    return results


def get_interview_stream(
    config_file: Path, interview_name: str, role: InterviewRole
) -> Optional[Path]:
    """
    Get the path to the video stream for the given interview and role.

    Args:
        config_file (Path): The path to the configuration file.
        interview_name (str): The name of the interview.
        role (InterviewRole): The role of the user.
    """
    of_path = get_openface_path(
        config_file=config_file, interview_name=interview_name, role=role
    )

    stream_query = f"""
        SELECT vs_path
        FROM openface
        WHERE of_processed_path = '{of_path}'
    """

    stream_path = db.fetch_record(config_file=config_file, query=stream_query)

    if stream_path is None:
        return None

    return Path(stream_path)


def get_interview_stream_from_openface_path(
    config_file: Path, of_path: Path
) -> Optional[Path]:
    """
    Get the path to the video stream for the given interview and role.
    Args:
        config_file (Path): The path to the configuration file.
        of_path (Path): The path to the openface directory.
    Returns:
        Optional[Path]: The path to the video stream if found, None otherwise.
    """

    stream_query = f"""
        SELECT vs_path
        FROM openface
        WHERE of_processed_path = '{of_path}'
    """

    stream_path = db.fetch_record(config_file=config_file, query=stream_query)

    if stream_path is None:
        return None

    return Path(stream_path)


def get_interview_duration(config_file: Path, interview_name: str) -> int:
    """
    Get the duration of the interview.

    Args:
        config_file (Path): The path to the configuration file.
        interview_name (str): The name of the interview.

    Returns:
        int: The duration of the interview in seconds.
    """
    stream_path = get_interview_stream(
        config_file=config_file,
        interview_name=interview_name,
        role=InterviewRole.SUBJECT,
    )

    query = f"""
    SELECT fm_duration
    FROM ffprobe_metadata
    WHERE fm_source_path = '{stream_path}';
    """

    results = db.fetch_record(config_file=config_file, query=query)

    if results is None:
        raise ValueError(f"No interview duration found for interview {interview_name}")

    return int(float(results))


def get_interview_path(interview_name: str, config_file: Path) -> Optional[Path]:
    """
    Get the path to the interview for the given interview name.

    Args:
        interview_name (str): The name of the interview.
        config_file (Path): The path to the configuration file.

    Returns:
        Optional[Path]: The path to the interview if found, None otherwise.
    """
    query = f"""
    SELECT interview_path
    FROM interviews
    WHERE interview_name = '{interview_name}'
    """

    result = db.fetch_record(config_file=config_file, query=query)

    if result is not None:
        interview_path = Path(result)

        return interview_path


def list_to_tuple(function: Callable) -> Any:
    """Custom decorator function, to convert list to a tuple."""

    def wrapper(*args, **kwargs) -> Any:
        args = tuple(tuple(x) if isinstance(x, list) else x for x in args)
        kwargs = {k: tuple(v) if isinstance(v, list) else v for k, v in kwargs.items()}
        result = function(*args, **kwargs)
        result = tuple(result) if isinstance(result, list) else result
        return result

    return wrapper


@list_to_tuple
@lru_cache(maxsize=32)
def fetch_openface_features(
    interview_name: str,
    subject_id: str,
    study_id: str,
    role: InterviewRole,
    cols: List[str],
    config_file: Path,
    only_success: bool = True,
) -> pd.DataFrame:
    """
    Fetches OpenFace features for a given OSIR ID and role.

    Args:
        osir_id (str): The OSIR ID to fetch features for.
        role (str): The role to fetch features for.
        cols (List[str]): The list of columns to fetch.
        config_file_path (str): The path to the configuration file.
        only_success (bool, optional): Whether to fetch only successful features. Defaults to True.

    Returns:
        pd.DataFrame: A DataFrame containing the fetched features.
    """

    if only_success:
        sql_query = f"""
            SELECT
                "{'", "'.join(cols)}"
            FROM openface_features
            WHERE success = TRUE AND
                interview_name = '{interview_name}' AND
                subject_id = '{subject_id}' AND
                study_id = '{study_id}' AND
                ir_role = '{role}';
        """
    else:
        sql_query = f"""
            SELECT
                "{'", "'.join(cols)}"
            FROM openface_features
            WHERE interview_name = '{interview_name}' AND
                subject_id = '{subject_id}' AND
                study_id = '{study_id}' AND
                ir_role = '{role}';
        """

    session_of_features = db.execute_sql(
        config_file=config_file, query=sql_query, db="openface_db"
    )

    return session_of_features


@list_to_tuple
@lru_cache(maxsize=32)
def fetch_openface_subject_distribution(
    subject_id: str, cols: List[str], config_file: Path
) -> pd.DataFrame:
    """
    Fetches the distribution of OpenFace features for a given subject in an interview.

    Args:
        interview_metadata (InterviewMetadata): Metadata for the interview.
        cols (List[str]): List of column names to fetch from the OpenFace features table.
        config_file_path (str): Path to the configuration file.

    Returns:
        pd.DataFrame: A DataFrame containing the distribution of OpenFace features
            for the given subject.
    """
    sql_query = f"""
        SELECT
            "{'", "'.join(cols)}"
        FROM openface_features AS openface
        WHERE subject_id = '{subject_id}' AND
            ir_role = 'subject' AND
            openface.success = True;
    """

    subject_of_features = db.execute_sql(
        config_file=config_file, query=sql_query, db="openface_db"
    )

    return subject_of_features


def get_study_visits_count(config_file: Path, study_id: str) -> Optional[int]:
    """
    Get the number of visits for a given study.

    Args:
        config_file (Path): The path to the configuration file.
        study_id (str): The ID of the study.

    Returns:
        Optional[int]: The number of visits if found, None otherwise.
    """
    query = f"""
    SELECT COUNT(DISTINCT interview_name) AS total_visits
    FROM interviews
    WHERE study_id = '{study_id}';
    """

    results = db.fetch_record(config_file=config_file, query=query)

    if results is None:
        return None

    return int(results)


def get_study_subjects_count(config_file: Path, study_id: str) -> Optional[int]:
    """
    Get the number of subjects for a given study.

    Args:
        config_file (Path): The path to the configuration file.
        study_id (str): The ID of the study.

    Returns:
        Optional[int]: The number of subjects if found, None otherwise.
    """
    query = f"""
    SELECT COUNT(DISTINCT subject_id) AS total_subjects
    FROM subjects
    WHERE study_id = '{study_id}';
    """

    results = db.fetch_record(config_file=config_file, query=query)

    if results is None:
        return None

    return int(results)


def fetch_openface_qc(
    interview_name: str, ir_role: InterviewRole, config_file: Path
) -> pd.DataFrame:
    """
    Get the successful frames percentage and confidence mean for a given interview and role.

    Args:
        interview_name (str): The name of the interview.
        ir_role (InterviewRole): The role of the user.
        config_file (Path): The path to the configuration file.

    Returns:
        pd.DataFrame: A DataFrame containing the successful frames percentage and confidence mean.
    """
    processed_path = get_openface_path(
        config_file=config_file, interview_name=interview_name, role=ir_role
    )

    sql_query = f"""
        SELECT
            sucessful_frames_percentage,
            successful_frames_confidence_mean
        FROM openface_qc
        WHERE of_processed_path = '{processed_path}'
    """

    df = db.execute_sql(config_file=config_file, query=sql_query)

    return df


def add_facial_expressivity_metric(
    df: pd.DataFrame, fau_cols: List[str]
) -> pd.DataFrame:
    """
    Adds the facial expressivity metric to a DataFrame containing OpenFace features.

    Facial expressivity is defined as the average of the action units.

    Args:
        df (pd.DataFrame): A DataFrame containing OpenFace features.

    Returns:
        pd.DataFrame: A DataFrame containing OpenFace features with the
            facial expressivity metric added.
    """

    df["facial_expressivity"] = df[fau_cols].mean(axis=1)

    return df


def construct_openface_metrics(session_openface_features: pd.DataFrame) -> Dict:
    """
    Computes the mean, standard deviation, and correlations of OpenFace features.

    Args:
        session_openface_features (pd.DataFrame): A DataFrame containing OpenFace features.

    Returns:
        Dict: A dictionary containing the mean, standard deviation, and
            correlations of OpenFace features.
    """
    pose_cols = constants.POSE_COLS
    au_cols = constants.AU_COLS

    openface_features = dict()
    start_time = session_openface_features["timestamp"].min()  # 00:00:00
    end_time = session_openface_features["timestamp"].max()  # 00:10:00

    try:
        start_time = datetime.combine(datetime.min, start_time)
        end_time = datetime.combine(datetime.min, end_time)
    except TypeError as e:
        # console.print(f"start_time: {start_time}")
        # console.print(f"end_time: {end_time}")
        # console.print(f"Skipping due to {e}")
        raise ValueError("start_time or end_time is None") from e

    duration = end_time - start_time
    openface_features["duration"] = duration
    openface_features["start_time"] = start_time.time()
    openface_features["end_time"] = end_time.time()

    session_openface_features.drop(columns=["timestamp"], inplace=True)

    session_openface_features = add_facial_expressivity_metric(
        session_openface_features, au_cols
    )

    of_cols = pose_cols + au_cols + ["facial_expressivity"]
    session_means = session_openface_features.mean(axis=0)
    session_std = session_openface_features.std(axis=0)

    means = dict()
    stds = dict()
    for col in of_cols:
        col_mean = session_means[col]
        col_std = session_std[col]

        means[col] = col_mean
        stds[col] = col_std

    openface_features["mean"] = means
    openface_features["std"] = stds

    au_features = session_openface_features[au_cols]
    au_corr = au_features.corr()

    correlations = dict()
    for i in range(len(au_cols)):
        for j in range(len(au_cols)):
            if i == j:
                continue

            correlations[au_cols[i] + "_vs_" + au_cols[j] + "_corr"] = au_corr.iloc[
                i, j
            ]

    openface_features["correlations"] = correlations

    return openface_features


def get_pdf_report_path(
    config_file: Path, interview_name: str, report_version: str
) -> Optional[Path]:
    """
    Get the path to the PDF report for the given interview

    Args:
        config_file (Path): The path to the configuration file.
        interview_name (str): The name of the interview.
        report_version (str): The version of the report.

    Returns:
        Path: The path to the PDF report.
    """
    query = f"""
    SELECT pr_path
    FROM pdf_reports
    WHERE interview_name = '{interview_name}' AND pr_version = '{report_version}';
    """

    results = db.fetch_record(config_file=config_file, query=query)

    if results is None:
        return None

    return Path(results)
