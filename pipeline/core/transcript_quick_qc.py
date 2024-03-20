"""
Helper functions for performing quick QC on transcripts.
"""

import heapq
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Optional, Union

import pandas as pd

from pipeline.helpers import db
from pipeline.models.transcript_quick_qc import TranscriptQuickQc
from pipeline.models.interview_roles import InterviewRole

logger = logging.getLogger(__name__)


def get_transcript_to_process(
    config_file: Path,
    study_id: str,
) -> Optional[Path]:
    """
    Fetch a random transcript to process, that has not been processed yet.

    Processed transcripts have a row in the  'transcript_quick_qc' table.

    Args:
        config_file (Path): Path to the config file.
        study_id (str): The study id.

    Returns:
        Optional[Path]: Path to the transcript to process.
    """

    query = f"""
        SELECT interview_file
        FROM interview_files
        INNER JOIN interviews ON interview_files.interview_path = interviews.interview_path
        WHERE interviews.study_id = '{study_id}' AND
            interview_files.interview_file_tags LIKE '%%transcript%%' AND
            interview_files.interview_file NOT IN (
                SELECT transcript_path FROM transcript_quick_qc
            )
        ORDER BY RANDOM()
        LIMIT 1
        """

    transcript = db.fetch_record(config_file=config_file, query=query)

    if transcript:
        return Path(transcript)

    return None


def transcript_to_df(transcript_path: Path) -> pd.DataFrame:
    """
    Reads a transcript file and returns a dataframe with the following columns:
    - start_time
    - end_time
    - speaker
    - text

    Expected format:
    ```test
        S1 00:00:02.350 Greetings, everyone. Welcome to the first session of the day.

        S2 00:00:06.000 Thank you for joining us.
    ```

    Args:
        transcript_path: Path, path to the transcript file

    Returns:
        pd.DataFrame
    """

    chunks = []

    with open(str(transcript_path), "r", encoding="utf-8") as f:
        lines = f.readlines()

    for line in lines:
        if not line.strip():
            continue

        try:
            stripped_line = line.strip()

            if stripped_line[0] != "S":
                logger.warning(f"Skipped parsing line: '{stripped_line}'")
                continue

            speaker, time, text = stripped_line.split(" ", 2)
        except ValueError:
            logger.error(f"Failed to parse line: '{stripped_line}'")
            raise
        chunks.append(
            {
                "start_time": time,
                "speaker": speaker,
                "text": text.strip(),
            }
        )

    df = pd.DataFrame(chunks)

    # end time is the start time of the next chunk
    df["end_time"] = df["start_time"].shift(-1)

    return df


def get_transcription_quick_qc(
    transcript_df: pd.DataFrame,
) -> Dict[str, Dict[str, Any]]:
    """
    Performs a quick quality check on the transcript dataframe.

    Metrics computed for each speaker:
    - num questions
    - num self references ('I')
    - num turns
    - avg turn length
    - total turn length

    Args:
        transcript_df: pd.DataFrame

    Returns:
        Dict[str, Dict[str, Any]]
    """
    speakers = transcript_df["speaker"].unique()

    results = {}

    for speaker in speakers:
        speaker_results = {}
        speaker_df = transcript_df[transcript_df["speaker"] == speaker].copy()
        speaker_df["turn_duration"] = pd.to_timedelta(
            speaker_df["end_time"]
        ) - pd.to_timedelta(speaker_df["start_time"])

        num_questions = speaker_df["text"].str.contains(r"\?").sum()
        num_self_references = speaker_df["text"].str.contains(r"\bI\b").sum()
        avg_turn_duration = speaker_df["turn_duration"].mean()
        sum_turn_duration = speaker_df["turn_duration"].sum()
        num_turns = speaker_df.shape[0]

        speaker_results["num_questions"] = num_questions
        speaker_results["num_self_references"] = num_self_references
        speaker_results["avg_turn_duration"] = avg_turn_duration
        speaker_results["num_turns"] = num_turns
        speaker_results["sum_turn_duration"] = sum_turn_duration

        results[speaker] = speaker_results

    return results


def calculate_confidence(
    qqc: Dict[str, Dict[str, Union[int, float, timedelta]]], role: InterviewRole
) -> Dict[str, float]:
    """
    Calculates the confidence of each speaker being the interviewer.

    Assumptions:
    - The interviewer asks the most questions
    - The subject speaks for the longest duration
    - The subject has the most turns
    - The subject has the most self-references

    Args:
        qqc: Dict[str, Dict[str, Union[int, float, timedelta]]] - the result of
            the quick quality check
        role: InterviewRole - the role to calculate confidence for

    Returns:
        Dict[str, float]
    """
    total_questions = 0
    total_turn_duration = timedelta(0)
    total_turns = 0
    total_self_references = 0
    for speaker, speaker_info in qqc.items():
        total_questions += speaker_info["num_questions"]  # type: ignore
        total_turn_duration += speaker_info["sum_turn_duration"]  # type: ignore
        total_turns += speaker_info["num_turns"]  # type: ignore
        total_self_references += speaker_info["num_self_references"]  # type: ignore

    confidences = {}
    for speaker, speaker_info in qqc.items():
        question_ratio = speaker_info["num_questions"] / total_questions
        turn_duration_ratio = (
            speaker_info["sum_turn_duration"].total_seconds()  # type: ignore
            / total_turn_duration.total_seconds()  # type: ignore
        )
        turn_ratio = speaker_info["num_turns"] / total_turns  # type: ignore
        self_reference_ratio = (
            speaker_info["num_self_references"] / total_self_references  # type: ignore
        )

        # Weigh each ratio according to its importance
        match role:
            case InterviewRole.INTERVIEWER:
                confidence = (
                    0.5 * question_ratio  # type: ignore
                    + 0.4 * (1 - self_reference_ratio)  # type: ignore
                    + 0.05 * (1 - turn_duration_ratio)  # type: ignore
                    + 0.05 * (1 - turn_ratio)  # type: ignore
                )
            case InterviewRole.SUBJECT:
                confidence = (
                    0.5 * (1 - question_ratio)  # type: ignore
                    + 0.4 * self_reference_ratio  # type: ignore
                    + 0.05 * turn_duration_ratio  # type: ignore
                    + 0.05 * turn_ratio  # type: ignore
                )
            case _:
                raise ValueError(f"Invalid role: {role}")

        confidences[speaker] = confidence

    return confidences


def add_speaker_roles_to_qqc(
    qqc: Dict[str, Dict[str, Any]],
) -> Dict[str, Dict[str, Any]]:
    """
    Adds speaker roles to the quick quality check results.

    Args:
        qqc: Dict[str, Dict[str, Any]]

    Returns:
        Dict[str, Dict[str, Any]]
    """
    for role in [InterviewRole.INTERVIEWER, InterviewRole.SUBJECT]:
        confidences = calculate_confidence(qqc=qqc, role=role)
        best_match = heapq.nlargest(1, confidences, key=confidences.get)[-1]  # type: ignore

        qqc[best_match]["role"] = role.value

        for speaker, confidence in confidences.items():
            qqc[speaker][f"{role.value.lower()}_confidence"] = confidence

    return qqc


def log_transcript_quick_qc(
    config_file: Path,
    transcript_path: Path,
    qqc: Dict[str, Dict[str, Any]],
    process_time: Optional[float] = None,
) -> None:
    """
    Logs the results of the quick quality check to the database.

    Args:
        config_file: Path, path to the config file
        transcript_path: Path, path to the transcript
        qqc: Dict[str, Dict[str, Any]]

    Returns:
        None
    """

    transcript_qqc = TranscriptQuickQc(
        transcript_path=transcript_path,
        speaker_metrics=qqc,
        process_time=process_time,
        timestamp=datetime.now(),
    )

    sql_query = transcript_qqc.insert_query()

    db.execute_queries(config_file=config_file, queries=[sql_query])

    return None
