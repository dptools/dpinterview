#!/usr/bin/env python
"""
Loads OpenFace Features into openface_db

Determines in Interview is ready for report generation.
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

import logging
from typing import List, Dict

from rich.logging import RichHandler
import pandas as pd
from tqdm import tqdm

from pipeline import orchestrator, data
from pipeline.helpers import utils, db, dpdash
from pipeline.helpers.timer import Timer
from pipeline.models.load_openface import LoadOpenface

MODULE_NAME = "load_openface"

logger = logging.getLogger(MODULE_NAME)
logargs = {
    "level": logging.DEBUG,
    # "format": "%(asctime)s - %(process)d - %(name)s - %(levelname)s - %(message)s",
    "format": "%(message)s",
    "handlers": [RichHandler(rich_tracebacks=True)],
}
logging.basicConfig(**logargs)

console = utils.get_console()


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
    WHERE interview_files.interview_name not in (
        SELECT interview_name FROM load_openface
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


def construct_load_openface(interview_name: str, of_runs: pd.DataFrame) -> LoadOpenface:
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

    if "interviewer" in roles_available:
        interviewer_of_path = of_runs[of_runs["ir_role"] == "interviewer"][
            "of_processed_path"
        ].iloc[0]

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
    datatypes = data.get_openface_datatypes(config_file, csv_file)
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


if __name__ == "__main__":
    config_file = utils.get_config_file_path()
    utils.configure_logging(
        config_file=config_file, module_name=MODULE_NAME, logger=logger
    )

    console.rule(f"[bold red]{MODULE_NAME}")
    logger.info(f"Using config file: {config_file}")

    config_params = utils.config(config_file, section="general")
    study_id = config_params["study"]

    COUNTER = 0

    logger.info("[bold green]Starting load_openface loop...", extra={"markup": True})

    while True:
        interview_name = get_interview_to_process(
            config_file=config_file, study_id=study_id
        )

        if interview_name is None:
            # Log if any files were processed
            if COUNTER > 0:
                data.log(
                    config_file=config_file,
                    module_name=MODULE_NAME,
                    message=f"Loaded OpenFace features for {COUNTER} interviews.",
                )
                COUNTER = 0

            # Snooze if no files to process
            orchestrator.snooze(config_file=config_file)
            continue

        COUNTER += 1

        logger.info(
            f"[cyan]Loading OpenFace features for {interview_name}...",
            extra={"markup": True},
        )

        of_runs = get_openface_runs(
            config_file=config_file, interview_name=interview_name
        )

        lof = construct_load_openface(interview_name=interview_name, of_runs=of_runs)
        lof = import_of_openface_db(config_file=config_file, lof=lof)

        log_load_openface(config_file=config_file, lof=lof)
