"""
Helper functions for disfluency detection.
"""

from pathlib import Path
import logging

import pandas as pd

from pipeline.helpers.transcribeme import constants


logger = logging.getLogger(__name__)


def parse_transcript_to_df(
    transcript: Path, time_format: str = "%H:%M:%S.%f"
) -> pd.DataFrame:
    """
    Reads transcripts of the following format:

    ```text
    speaker: 00:00:00.000 transcript
    ```

    and converts it to a DataFrame with columns:
    - turn
    - speaker
    - time
    - transcript

    Note: This module relies on specific 'conventions' used
    by TranscribeMe's verbatim transcripts.

    Args:
        transcript (Path): Path to the transcript file

    Returns:
        pd.DataFrame: DataFrame with the transcript
    """

    with open(transcript, "r", encoding="utf-8") as f:
        lines = f.readlines()

    data = []
    turn_idx = 1
    for line in lines:
        try:
            speaker, time, text = line.split(" ", 2)
        except ValueError:
            continue

        speaker = speaker[:-1]
        text = text.strip()
        element_data = {
            "turn": turn_idx,
            "speaker": speaker,
            "time": time,
            "transcript": text,
        }
        data.append(element_data)
        turn_idx += 1

    if len(data) == 0:
        logger.error(f"Error parsing transcript in {transcript}")
        raise ValueError(f"Error parsing transcript in {transcript}")
    df = pd.DataFrame(data)

    # Add a end_time column, by shifting the time column by one
    df["end_time"] = df["time"].shift(-1)

    # Compute the duration of each turn with millisecond-level precision
    try:
        df["duration_ms"] = pd.to_datetime(
            df["end_time"], format=time_format
        ) - pd.to_datetime(df["time"], format=time_format)
    except ValueError as e:
        # Attempt falling back to '%H:%M:%S.%f' format
        try:
            df["duration_ms"] = pd.to_datetime(
                df["end_time"], format="%H:%M:%S.%f"
            ) - pd.to_datetime(df["time"], format="%H:%M:%S.%f")
        except ValueError:
            logger.error(f"Error parsing time in {transcript}")
            raise e

    df["duration_ms"] = df["duration_ms"].dt.total_seconds() * 1000

    # Replace nan values with 0 on 'duration' column
    df["duration_ms"].fillna(0, inplace=True)

    # cast the turn, duration columns to int
    df["turn"] = df["turn"].astype(int)
    df["duration_ms"] = df["duration_ms"].astype(int)

    df.drop(columns=["end_time"], inplace=True)

    return df


def count_words_zh(text: str) -> int:
    """
    Counts the number of words in a Chinese text.

    Args:
        text (str): Chinese text

    Returns:
        int: Number of words in the text
    """
    zh_punctuations = constants.zh_punctuations_mapping.keys()
    for punctuation in zh_punctuations:
        text = text.replace(punctuation, "")

    text = text.replace(" ", "")
    text = text.replace("[foreign]", "F")
    text = text.replace("{REDACTED}", "R")
    return len(text)


def add_basic_features(df: pd.DataFrame, language: str) -> pd.DataFrame:
    """
    Adds basic features to the DataFrame:
    - Number of words
    - Number of worder per second

    Args:
        df (pd.DataFrame): DataFrame with the transcript

    Returns:
        pd.DataFrame: DataFrame with the added features
    """
    if language == "zh":
        df["n_words"] = df["transcript"].apply(lambda x: count_words_zh(x))
    else:
        df["n_words"] = df["transcript"].apply(lambda x: len(x.split()))
    df["n_words_per_sec"] = df["n_words"] / (df["duration_ms"] / 1000)
    return df
