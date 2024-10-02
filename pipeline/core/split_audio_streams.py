"""
Helper functions to split audio streams.
"""

import logging
from pathlib import Path
from typing import List, Optional

from pipeline.helpers import db, dpdash
from pipeline.models.audio_streams import AudioStream

logger = logging.getLogger(__name__)


def get_file_to_process(config_file: Path, study_id: str) -> Optional[Path]:
    """
    Fetch a file to process from the database.

    - Fetches a file that has not been processed yet and is part of the study.

    Args:
        config_file (Path): Path to config file
    """
    query = f"""
    SELECT destination_path FROM decrypted_files
    INNER JOIN interview_files ON decrypted_files.source_path = interview_files.interview_file
    INNER JOIN interviews USING (interview_path)
    WHERE interview_files.interview_file_tags LIKE '%%audio%%' AND
        decrypted_files.destination_path NOT IN (
            SELECT as_source_path FROM audio_streams
        ) AND
        interviews.study_id = '{study_id}'
    ORDER BY RANDOM()
    LIMIT 1
    """

    source_path = db.fetch_record(config_file=config_file, query=query)
    if source_path is None:
        return None

    return Path(source_path)


def check_if_ffprobe_data_exists(source_path: Path, config_file: Path) -> bool:
    """
    Check if ffprobe data exists for the source path.

    Args:
        source_path (Path): Path to the source file
        config_file (Path): Path to the config file

    Returns:
        bool: True if ffprobe data exists, False otherwise
    """
    query = f"""
    SELECT COUNT(*) FROM ffprobe_metadata
    WHERE fm_source_path = '{source_path}'
    """
    count = db.fetch_record(config_file=config_file, query=query)
    count = int(count)  # type: ignore

    return count > 0


def get_audio_streams_count(source_path: Path, config_file: Path) -> int:
    """
    Return the number of audio streams for the source path.

    Args:
        source_path (Path): Path to the source file
        config_file (Path): Path to the config file

    Returns:
        int: Number of audio streams
    """
    query = f"""
    SELECT COUNT(*) FROM ffprobe_metadata_audio
    WHERE fma_source_path = '{source_path}'
    """
    count = db.fetch_record(config_file=config_file, query=query)
    if count is None:
        return 0
    count = int(count)  # type: ignore

    return count


def construct_stream_path(source_path: Path, comment: str, suffix: str) -> Path:
    """
    Constructs a dpdash compliant stream path

    Args:
        source_path (Path): Path to the source file
        comment (str): Comment to be added to the stream
        suffix (str): Suffix of the stream
    """
    dpdash_dict = dpdash.parse_dpdash_name(source_path.name)
    if dpdash_dict["optional_tags"] is None:
        optional_tag: List[str] = []
    else:
        optional_tag = dpdash_dict["optional_tags"]  # type: ignore

    optional_tag.append(comment)
    dpdash_dict["optional_tags"] = optional_tag

    dpdash_dict["category"] = "audio"

    dp_dash_name = dpdash.get_dpdash_name_from_dict(dpdash_dict)
    stream_path = (
        source_path.parent
        / "streams"
        / source_path.name.split(".")[0]
        / f"{dp_dash_name}.{suffix}"
    )

    # Create streams directory if it doesn't exist
    stream_path.parent.mkdir(parents=True, exist_ok=True)

    return stream_path


def log_audio_streams(config_file: Path, streams: List[AudioStream]) -> None:
    """
    Log streams to database.

    Args:
        config_file (Path): Path to config file
        streams (List[AudioStream]): List of streams
    """
    sql_queries = [stream.to_sql() for stream in streams]

    logger.info("Inserting streams into DB", extra={"markup": True})
    db.execute_queries(config_file=config_file, queries=sql_queries)
