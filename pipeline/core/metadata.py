"""
Metadata Module
"""

import logging
from pathlib import Path
from typing import Dict, Optional

from pipeline.helpers import db
from pipeline.models.ffprobe_metadata import FfprobeMetadata

logger = logging.getLogger(__name__)


def get_file_to_process(config_file: Path, study_id: str) -> Optional[str]:
    """
    Fetch a file to process from the database.

    Fetches a file that has not been processed yet and is part of the study.

    Args:
        config_file (Path): Path to config file
        study_id (str): Study ID
    """
    sql_query = f"""
        SELECT destination_path
        FROM decrypted_files
        WHERE destination_path NOT IN (
            SELECT fm_source_path
            FROM ffprobe_metadata
        ) AND source_path IN (
            SELECT interview_file
            FROM interview_files
            LEFT JOIN interview_parts USING (interview_path)
            LEFT JOIN interviews USING (interview_name)
            WHERE study_id = '{study_id}'
        ) AND decrypted = TRUE
        AND requested_by = 'fetch_video'
        ORDER BY RANDOM()
        LIMIT 1;
    """

    result = db.fetch_record(config_file=config_file, query=sql_query)

    if result is None:
        sql_query = f"""
            SELECT vs_path
            FROM video_streams
            WHERE vs_path NOT IN (
                SELECT fm_source_path
                FROM ffprobe_metadata
            ) AND video_path IN (
                SELECT destination_path
                FROM decrypted_files
                LEFT JOIN interview_files ON interview_files.interview_file = decrypted_files.source_path
                LEFT JOIN interview_parts USING (interview_path)
                JOIN interviews USING (interview_name)
                WHERE interviews.study_id = '{study_id}'
            )
            ORDER BY RANDOM()
            LIMIT 1;
        """

        result = db.fetch_record(config_file=config_file, query=sql_query)

    return result


def log_metadata(
    source: Path, metadata: Dict, config_file: Path, requested_by: str
) -> None:
    """
    Logs metadata to the database.

    Args:
        source (Path): Path to source file
        metadata (Dict): Metadata to log
        config_file (Path): Path to config file
    """
    ffprobe_metadata = FfprobeMetadata(
        source_path=source, metadata=metadata, requested_by=requested_by
    )

    if "streams" not in metadata:
        # FfprobeMetadata.to_sql() falls back to inserting a placeholder row
        # (source_path/requested_by only, no stream data) when this happens -
        # that placeholder permanently satisfies get_file_to_process()'s
        # "not already in ffprobe_metadata" check above, so this file is never
        # retried. Record it so it's queryable instead of silently stuck.
        db.record_failure(
            config_file=config_file,
            stage="metadata",
            identifier=str(source),
            error="ffprobe metadata has no 'streams' key; inserting placeholder "
            "row only - this file will not be retried",
            identifier_type="file_path",
        )

    sql_queries = ffprobe_metadata.to_sql()

    logger.info("Logging metadata...", extra={"markup": True})
    db.execute_queries(config_file=config_file, queries=sql_queries, show_commands=False, silent=True)
