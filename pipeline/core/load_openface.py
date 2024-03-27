"""
Helper functions for loading OpenFace features into the database.
"""

import logging
from pathlib import Path
from typing import Dict, List

import pandas as pd
from tqdm import tqdm

from pipeline import core
from pipeline.core import metadata
from pipeline.helpers import db, dpdash, ffprobe
from pipeline.helpers.timer import Timer
from pipeline.models.load_openface import LoadOpenface

logger = logging.getLogger(__name__)


def get_interview_to_process(config_file: Path, study_id: str):
    """
    Fetch an interview to process from the database.

    - Fetches an interview that has not been processed yet.
        - Must be processed by OpenFace.

    Args:
        config_file (Path): Path to the config file.
        study_id (str): Study ID.
    """
    query = f"""
    SELECT interview_files.interview_name
    FROM openface
    INNER JOIN video_streams USING (vs_path)
    INNER JOIN (
        SELECT decrypted_files.destination_path, interviews.interview_name
        FROM interview_files
        JOIN decrypted_files
        ON interview_files.interview_file = decrypted_files.source_path
        JOIN interviews
        ON interview_files.interview_path = interviews.interview_path
    ) AS interview_files
    ON video_streams.video_path = interview_files.destination_path
    WHERE interview_files.interview_name NOT IN (
        SELECT interview_name FROM load_openface
    ) AND interview_files.interview_name NOT IN (  -- Exclude interviews that are yet to be processed by OpenFace
        SELECT if.interview_name
        FROM video_streams AS vs
        INNER JOIN (
            SELECT decrypted_files.destination_path, interview_files.interview_file_tags, interviews.interview_name
            FROM interview_files
            JOIN decrypted_files
                ON interview_files.interview_file = decrypted_files.source_path
            join interviews using (interview_path)
        ) AS if
        ON vs.video_path = if.destination_path
        WHERE vs.vs_path NOT IN (
            SELECT vs_path FROM openface
        ) AND vs.video_path IN (
            SELECT destination_path FROM decrypted_files
            JOIN interview_files ON interview_files.interview_file = decrypted_files.source_path
            JOIN interviews USING (interview_path)
            WHERE interviews.study_id = '{study_id}'
        )
    ) AND video_streams.video_path IN (
        SELECT destination_path FROM decrypted_files
        JOIN interview_files ON interview_files.interview_file = decrypted_files.source_path
        JOIN interviews USING (interview_path)
        WHERE interviews.study_id = '{study_id}'
    )
    ORDER BY RANDOM()
    LIMIT 1
    """

    df = db.execute_sql(config_file=config_file, query=query)

    if df.empty:
        return None

    interview_name = df["interview_name"].iloc[0]

    return interview_name


def get_openface_runs(config_file: Path, interview_name: str) -> pd.DataFrame:
    """
    Retrives all OpenFace runs for an interview. (Interviewer and Subject)

    Args:
        config_file (Path): Path to the config file.
        interview_name (str): The name of the interview.
    """
    query = f"""
    SELECT openface.of_processed_path, openface.ir_role, interview_files.interview_name
    FROM openface
    INNER JOIN video_streams USING (vs_path)
    INNER JOIN (
        SELECT decrypted_files.destination_path, interviews.interview_name
        FROM interview_files
        JOIN decrypted_files
        ON interview_files.interview_file = decrypted_files.source_path
        JOIN interviews
        ON interview_files.interview_path = interviews.interview_path
    ) AS interview_files
    ON video_streams.video_path = interview_files.destination_path
    WHERE interview_files.interview_name = '{interview_name}'
    """

    df = db.execute_sql(config_file=config_file, query=query)

    return df


def construct_load_openface(
    config_file: Path, interview_name: str, of_runs: pd.DataFrame
) -> LoadOpenface:
    """
    Constructs a LoadOpenface object.

    Args:
        interview_name (str): The name of the interview.
        of_runs (pd.DataFrame): A DataFrame containing the OpenFace runs for the interview.
    """
    notes = None
    report_generation_possible = True

    subject_of_path = None
    interviewer_of_path = None

    dp_dict = dpdash.parse_dpdash_name(interview_name)
    subject_id = dp_dict["subject"]
    study_id = dp_dict["study"]

    if subject_id is None or study_id is None:
        logger.error(f"Could not parse subject and study from {interview_name}")
        raise ValueError(f"Could not parse subject and study from {interview_name}")

    if of_runs.empty:
        notes = "No OpenFace runs found"
        logger.warning(f"{notes} for {interview_name}")
        report_generation_possible = False

    if len(of_runs) > 2:
        notes = "More than 2 OpenFace runs found, Skip Report generation"
        logger.warning(f"{notes} for {interview_name}")
        report_generation_possible = False

    roles_available = of_runs["ir_role"].unique().tolist()

    if "subject" in roles_available:
        subject_of_path = of_runs[of_runs["ir_role"] == "subject"][
            "of_processed_path"
        ].iloc[0]
        vs_path = core.get_interview_stream_from_openface_path(
            config_file=config_file,
            of_path=subject_of_path,
        )
        metadata_dict = ffprobe.get_metadata(
            file_path_to_process=vs_path, config_file=config_file  # type: ignore
        )

        metadata.log_metadata(
            source=vs_path,  # type: ignore
            metadata=metadata_dict,
            config_file=config_file,
        )

    if "interviewer" in roles_available:
        interviewer_of_path = of_runs[of_runs["ir_role"] == "interviewer"][
            "of_processed_path"
        ].iloc[0]
        vs_path = core.get_interview_stream_from_openface_path(
            config_file=config_file,
            of_path=interviewer_of_path,
        )
        metadata_dict = ffprobe.get_metadata(
            file_path_to_process=vs_path, config_file=config_file  # type: ignore
        )

        metadata.log_metadata(
            source=vs_path,  # type: ignore
            metadata=metadata_dict,
            config_file=config_file,
        )

    lof = LoadOpenface(
        interview_name=interview_name,
        subject_id=subject_id,  # type: ignore
        study_id=study_id,  # type: ignore
        subject_of_processed_path=subject_of_path,  # type: ignore
        interviewer_of_processed_path=interviewer_of_path,
        lof_notes=notes,
        lof_report_generation_possible=report_generation_possible,
    )

    return lof


def construct_insert_queries(
    config_file: Path,
    interview_name: str,
    role: str,
    subject_id: str,
    study_id: str,
    csv_file: Path,
) -> List[str]:
    """
    Constructs a list of SQL insert queries for OpenFace features from a CSV file.

    Args:
        interview_name (str): The name of the interview.
        role (str): The role of the participant.
        subject_id (str): The subject ID.
        csv_file (str): The path to the CSV file containing the OpenFace features.

    Returns:
        List[str]: A list of SQL insert queries.
    """
    df = pd.read_csv(csv_file, on_bad_lines="skip")

    # Get datatypes
    datatypes = core.get_openface_datatypes(config_file, csv_file)
    cols = df.columns

    # Cast data to correct datatype
    # drop NaN values
    df = df.dropna()
    for col in cols:
        datatype = datatypes[col]

        try:
            match datatype:
                case "INTEGER":
                    df[col] = df[col].astype(int)
                case "BOOLEAN":
                    df[col] = df[col].astype(bool)
                case "TIME":
                    df[col] = pd.to_datetime(df[col], unit="s").dt.time
                case _:
                    pass
        except ValueError as e:
            print(f"Error casting {col} with value {df[col]} to {datatype}: {e}")

    queries: List[str] = []

    for _, row in tqdm(df.iterrows(), total=len(df)):
        vals: Dict[str, str] = {}

        for col in cols:
            vals[col] = str(row[col])

        query = f"""
            INSERT INTO openface_features (
                interview_name,
                ir_role,
                subject_id,
                study_id,
                {", ".join(['"' + col + '"' for col in cols])}
            ) VALUES (
                '{interview_name}',
                '{role}',
                '{subject_id}',
                '{study_id}',
                {", ".join(["'" + vals[col] + "'" for col in cols])}
            ) ON CONFLICT (interview_name, ir_role, frame, face_id) DO NOTHING;
        """

        queries.append(query)

    return queries


def import_of_openface_db(config_file: Path, lof: LoadOpenface) -> LoadOpenface:
    """
    Imports OpenFace features into openface_db.

    Args:
        config_file (Path): Path to the config file.
        lof (LoadOpenface): LoadOpenface object.
    """
    queries: List[str] = []

    if lof.lof_report_generation_possible is True:
        with Timer() as timer:
            if lof.interviewer_of_processed_path:
                logger.info(
                    f"Importing OpenFace features for {lof.interview_name} interviewer"
                )
                csv_files_f = Path(lof.interviewer_of_processed_path).glob("*.csv")
                csv_files = sorted(csv_files_f)
                if len(csv_files) > 1:
                    message = f"More than 1 OpenFace CSV file found for \
{lof.interview_name} interviewer"
                    logger.error(message)
                    raise ValueError(message)

                csv_file = csv_files[0]
                queries.extend(
                    construct_insert_queries(
                        config_file=config_file,
                        interview_name=lof.interview_name,
                        role="interviewer",
                        subject_id=lof.subject_id,
                        study_id=lof.study_id,
                        csv_file=csv_file,
                    )
                )

            if lof.subject_of_processed_path:
                logger.info(
                    f"Importing OpenFace features for {lof.interview_name} subject"
                )
                csv_files_f = Path(lof.subject_of_processed_path).glob("*.csv")
                csv_files = sorted(csv_files_f)
                if len(csv_files) > 1:
                    message = f"More than one OpenFace CSV file found for \
{lof.interview_name} subject"
                    logger.error(message)
                    raise ValueError(message)

                csv_file = csv_files[0]
                queries.extend(
                    construct_insert_queries(
                        config_file=config_file,
                        interview_name=lof.interview_name,
                        role="subject",
                        subject_id=lof.subject_id,
                        study_id=lof.study_id,
                        csv_file=csv_file,
                    )
                )

        logger.info(
            f"Importing OpenFace features to openface_db for {lof.interview_name}"
        )
        db.execute_queries(
            config_file=config_file,
            queries=queries,
            show_commands=False,
            show_progress=True,
            db="openface_db",
        )

        lof.lof_process_time = timer.duration

    return lof


def log_load_openface(config_file: Path, lof: LoadOpenface) -> None:
    """
    Logs the results of the (another) OpenFace QC to the database.

    Args:
        config_file (Path): Path to the config file.
        lof (LoadOpenface): LoadOpenface object.
    """
    query = lof.to_sql()

    logger.info(f"Logging load_openface for {lof.interview_name}")
    db.execute_queries(config_file=config_file, queries=[query], show_commands=True)
