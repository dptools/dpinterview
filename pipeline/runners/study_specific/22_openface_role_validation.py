#!/usr/bin/env python
"""
Validate the roles of the speakers in the OpenFace output.

Compares the roles assigned by transcript_quick_qc with OpeFace FAU
activations.
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

import argparse
import logging
from typing import Any, Dict, Optional
from datetime import datetime

from rich.logging import RichHandler
import pandas as pd
import numpy as np

from pipeline import orchestrator
from pipeline.helpers import cli, utils, db
from pipeline.core import transcript_quick_qc
from pipeline.models import InterviewRole
from pipeline.models.fau_role_validation import FauRoleValidation

MODULE_NAME = "pipeline.runners.study_specific.22_openface_role_validation"
INSTANCE_NAME = MODULE_NAME

logger = logging.getLogger(MODULE_NAME)
logargs = {
    "level": logging.DEBUG,
    # "format": "%(asctime)s - %(process)d - %(name)s - %(levelname)s - %(message)s",
    "format": "%(message)s",
    "handlers": [RichHandler(rich_tracebacks=True)],
}
logging.basicConfig(**logargs)

console = utils.get_console()


def get_interview_name_to_process(config_file: Path, study: str) -> Optional[str]:
    """
    Fetches an interview name to process from the database.

    Args:
        config_file (Path): Path to the config file.
        study (str): The study ID.

    Returns:
        Optional[str]: The interview name to process.
    """
    query = f"""
    SELECT load_openface.interview_name
    FROM load_openface
    INNER JOIN interviews ON load_openface.interview_name = interviews.interview_name
    INNER JOIN interview_files ON interview_files.interview_path = interviews.interview_path
    INNER JOIN transcript_quick_qc ON transcript_quick_qc.transcript_path = interview_files.interview_file
    WHERE
        interviews.study_id = '{study}' AND
        load_openface.interviewer_of_processed_path IS NOT NULL AND
        transcript_path IS NOT NULL AND
        load_openface.interview_name NOT IN (
            SELECT interview_name FROM fau_role_validation
        )
    ORDER BY RANDOM()
    LIMIT 1
    """

    interview_name = db.fetch_record(config_file=config_file, query=query)

    return interview_name


def fetch_openface_features_by_role(
    interview_name: str, role: InterviewRole, config_file: Path, fau: str = "AU25_c"
) -> pd.DataFrame:
    """
    Fetches the passed in FAU features for the passed in role.

    Args:
        interview_name (str): The interview name.
        role (InterviewRole): The role to fetch the features for.
        config_file (Path): Path to the config file.
        fau (str): The FAU to fetch.

    Returns:
        pd.DataFrame: The OpenFace features.
    """
    sql = f"""
        SELECT "timestamp", "{fau}" AS "{fau}_{role}"
        FROM openface_features
        WHERE interview_name = '{interview_name}' AND
            ir_role = '{role.value}';
    """

    df = db.execute_sql(config_file=config_file, query=sql, db="openface_db")
    return df


def fetch_openface_features(
    interview_name: str, config_file: Path, fau: str = "AU25_c"
) -> pd.DataFrame:
    """
    Fetches the passed in FAU features for both the interviewer and subject,
    and merges them on the timestamp.

    Args:
        interview_name (str): The interview name.

    Returns:

    """
    roles = [InterviewRole.SUBJECT, InterviewRole.INTERVIEWER]
    dfs = []

    for role in roles:
        df = fetch_openface_features_by_role(
            interview_name=interview_name, role=role, fau=fau, config_file=config_file
        )
        dfs.append(df)

    df = pd.merge(dfs[0], dfs[1], on="timestamp", how="outer")

    def time_to_float(time_value: datetime):
        return (
            time_value.hour * 3600
            + time_value.minute * 60
            + time_value.second
            + time_value.microsecond / 1000000
        )

    df["timestamp"] = df["timestamp"].apply(time_to_float)
    df["timestamp"] = df["timestamp"].apply(lambda x: x * 1000)

    return df


def compute_fau_speaker_metrics(
    transcript_df: pd.DataFrame, of_df: pd.DataFrame, fau_to_use: str = "AU25_c"
) -> Dict[str, Any]:
    """
    Counts the FAU activations for each speaker in the transcript.

    Args:
        transcript_df (pd.DataFrame): The transcript data.
        of_df (pd.DataFrame): The OpenFace data.

    Returns:
        Dict[str, Any]: The FAU derived metrics.
    """

    def time_to_float(time_value):
        return (
            time_value.hour * 3600
            + time_value.minute * 60
            + time_value.second
            + time_value.microsecond / 1000000
        )

    transcript_df["start_time"] = pd.to_datetime(transcript_df["start_time"])
    transcript_df["end_time"] = pd.to_datetime(transcript_df["end_time"])

    transcript_df["start_time"] = transcript_df["start_time"].apply(time_to_float)
    transcript_df["start_time"] = transcript_df["start_time"].apply(lambda x: x * 1000)

    transcript_df["end_time"] = transcript_df["end_time"].apply(time_to_float)
    transcript_df["end_time"] = transcript_df["end_time"].apply(lambda x: x * 1000)

    speakers = transcript_df["speaker"].unique()
    int_activation_count = dict.fromkeys(speakers, 0)
    pt_activation_count = dict.fromkeys(speakers, 0)
    for speaker in speakers:
        df_speaker = transcript_df[transcript_df["speaker"] == speaker]

        # Check if speaker matches more with subject or interviewer
        # If more with subject, then mark speaker as subject
        # If more with interviewer, then mark speaker as interviewer

        for _, row in df_speaker.iterrows():
            start = row["start_time"]
            end = row["end_time"]

            of_feature = of_df[
                (of_df["timestamp"] >= start) & (of_df["timestamp"] <= end)
            ]

            of_int_count = of_feature[f"{fau_to_use}_interviewer"].sum()
            of_part_count = of_feature[f"{fau_to_use}_subject"].sum()

            if of_int_count > of_part_count:
                int_activation_count[speaker] += 1
            else:
                pt_activation_count[speaker] += 1

    # convert int_activation_count and pt_activation_count to percentages
    int_activation_percent = dict.fromkeys(speakers, 0)
    pt_activation_percent = dict.fromkeys(speakers, 0)
    for speaker in speakers:
        int_activation_percent[speaker] = int_activation_count[speaker] / (  # type: ignore
            int_activation_count[speaker] + pt_activation_count[speaker]
        )
        pt_activation_percent[speaker] = pt_activation_count[speaker] / (  # type: ignore
            int_activation_count[speaker] + pt_activation_count[speaker]
        )

    results = {}
    for speaker in speakers:
        results[speaker] = {
            "interviewer_activation_count": int_activation_count[speaker],
            "subject_activation_count": pt_activation_count[speaker],
            "interviewer_activation_percent": int_activation_percent[speaker],
            "subject_activation_percent": pt_activation_percent[speaker],
        }

    return results


def assign_roles_to_fau_derived_metrics(fau_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Assigns roles to the speakers based on the FAU derived metrics.

    Args:
        fau_data (Dict[str, Any]): The FAU derived metrics.

    Returns:
        Dict[str, Any]: The FAU derived metrics with roles assigned.
    """
    # Compute mean and standard deviation for both interviewer and subject activation counts
    interviewer_counts = [
        stats["interviewer_activation_count"] for stats in fau_data.values()
    ]
    subject_counts = [stats["subject_activation_count"] for stats in fau_data.values()]

    interviewer_mean = np.mean(interviewer_counts)
    interviewer_std = np.std(interviewer_counts)

    subject_mean = np.mean(subject_counts)
    subject_std = np.std(subject_counts)

    # Define a multiplier for standard deviation to determine threshold
    multiplier = 1

    # Compute threshold values
    interviewer_threshold = interviewer_mean + multiplier * interviewer_std  # type: ignore
    subject_threshold = subject_mean + multiplier * subject_std  # type: ignore

    # Assign roles based on threshold values
    for _, stats in fau_data.items():
        if stats["interviewer_activation_count"] >= interviewer_threshold:
            stats["role"] = "interviewer"
        elif stats["subject_activation_count"] >= subject_threshold:
            stats["role"] = "subject"
        else:
            continue

    return fau_data


def check_match(transcript_qqc: Dict[str, Any], fau_data: Dict[str, Any]) -> bool:
    """
    Checks if the roles assigned by transcript_quick_qc match the roles assigned
    by the OpenFace FAU activations.

    Args:
        transcript_qqc (Dict[str, Any]): The transcript_quick_qc data.
        fau_data (Dict[str, Any]): The OpenFace FAU data.

    Returns:
        bool: Whether the roles match or not.
    """
    for speaker, stats in fau_data.items():
        if "role" not in stats and "role" not in transcript_qqc[speaker]:
            continue
        try:
            if transcript_qqc[speaker]["role"] != stats["role"]:
                return False
        except KeyError:
            return False

    return True


def log_fau_role_validation(
    fau_role_validation: FauRoleValidation, config_file: Path
) -> None:
    """
    Logs the FauRoleValidation object to the database.

    Args:
        fau_role_validation (FauRoleValidation): The FauRoleValidation object.
        config_file (Path): Path to the config file.
    """

    query = fau_role_validation.to_sql()

    db.execute_queries(config_file=config_file, queries=[query])


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="fetch_video", description="Fetch video files for decryption."
    )
    parser.add_argument(
        "-c", "--config", type=str, help="Path to the config file.", required=False
    )
    parser.add_argument(
        "-n",
        "--num_files_to_request",
        type=int,
        help="Number of files to request for decryption.",
        required=False,
    )

    args = parser.parse_args()

    # Check if parseer has config file
    if args.config:
        config_file = Path(args.config).resolve()
        if not config_file.exists():
            logger.error(f"Error: Config file '{config_file}' does not exist.")
            sys.exit(1)
    else:
        if cli.confirm_action("Using default config file."):
            config_file = utils.get_config_file_path()
        else:
            sys.exit(1)

    utils.configure_logging(
        config_file=config_file, module_name=MODULE_NAME, logger=logger
    )

    console.rule(f"[bold red]{MODULE_NAME}")
    logger.info(f"Using config file: {config_file}")

    config_params = utils.config(config_file, section="general")
    studies = orchestrator.get_studies(config_file=config_file)

    study_id = studies[0]
    logger.info(f"Starting with study: {study_id}", extra={"markup": True})

    COUNTER = 0

    while True:
        interview_name = get_interview_name_to_process(
            config_file=config_file, study=study_id
        )

        logger.info(
            f"Processing interview: [cyan]{interview_name}", extra={"markup": True}
        )

        if interview_name is None:
            if study_id == studies[-1]:
                # Log if any files were processed
                if COUNTER > 0:
                    orchestrator.log(
                        config_file=config_file,
                        module_name=MODULE_NAME,
                        message=f"Validated roles for {COUNTER} interviews.",
                    )
                    COUNTER = 0

                # Snooze if no files to process
                orchestrator.snooze(config_file=config_file)
                study_id = studies[0]
                logger.info(
                    f"Restarting with study: {study_id}", extra={"markup": True}
                )
                continue
            else:
                study_id = studies[studies.index(study_id) + 1]
                logger.info(f"Switching to study: {study_id}", extra={"markup": True})
                continue

        COUNTER += 1

        logger.info(f"Fetching OpenFace features for {interview_name}")
        of_df = fetch_openface_features(
            interview_name=interview_name, config_file=config_file
        )
        transcript_qqc = transcript_quick_qc.get_transcript_qqc(
            interview_name=interview_name, config_file=config_file
        )

        if transcript_qqc is None:
            logger.error(f"Skipping {interview_name} as no transcript_quick_qc found.")
            continue

        turn_df = transcript_quick_qc.fetch_turn_df(
            interview_name=interview_name, config_file=config_file
        )

        if turn_df is None:
            logger.error(
                f"Skipping {interview_name} as no turns found from the transcript."
            )
            continue

        fau_data = compute_fau_speaker_metrics(
            transcript_df=turn_df, of_df=of_df, fau_to_use="AU25_c"
        )
        fau_data = assign_roles_to_fau_derived_metrics(fau_data=fau_data)

        match = check_match(transcript_qqc=transcript_qqc, fau_data=fau_data)

        if match:
            logger.info(
                f"[green]Roles match for {interview_name}.", extra={"markup": True}
            )
        else:
            logger.warning(
                f"[red]Roles do not match for {interview_name}.", extra={"markup": True}
            )

        fau_role_validation = FauRoleValidation(
            interview_name=interview_name,
            fau_metrics=fau_data,
            matches_with_transcript=match,
        )

        logger.info(f"Logging FAU role validation for {interview_name}")
        log_fau_role_validation(
            fau_role_validation=fau_role_validation, config_file=config_file
        )
