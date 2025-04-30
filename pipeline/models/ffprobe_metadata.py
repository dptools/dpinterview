#!/usr/bin/env python
"""
FfprobeMetadata Model
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

import logging
import math
from datetime import datetime
from typing import Any, Dict, List, Optional

from pipeline.helpers import db, utils
from pipeline.models.interview_roles import InterviewRole

logger = logging.getLogger(__name__)
console = utils.get_console()


def aspect_ratio(width, height):
    """
    Calculate the aspect ratio of a video based on its width and height.

    Parameters:
    width (int): The width of the video.
    height (int): The height of the video.

    Returns:
    str: The aspect ratio in the format "width:height".
    """
    width = int(width)
    height = int(height)
    gcd = math.gcd(width, height)

    width_ratio = str(width // gcd)
    height_ratio = str(height // gcd)

    return width_ratio + ":" + height_ratio


def get_metric(metric: str, metadata: Dict[str, Any]) -> Optional[str]:
    """
    Get a metric from the ffprobe metadata.

    Args:
        metric (str): The metric to get.
        metadata (Dict[str, Any]): The ffprobe metadata.

    Returns:
        Optional[str]: The value of the metric or None if the metric does not exist.
    """
    if metric in metadata:
        return metadata[metric]
    else:
        if metric == "display_aspect_ratio":
            if "width" in metadata and "height" in metadata:
                width = metadata["width"]
                height = metadata["height"]

                return aspect_ratio(width, height)
        return None


def metric_or_null(metric: str, metadata: Dict[str, Any]) -> str:
    """
    Get a metric from the ffprobe metadata or return NULL if the metric does not exist.

    Args:
        metric (str): The metric to get.
        metadata (Dict[str, Any]): The ffprobe metadata.

    Returns:
        str: The value of the metric or NULL if the metric does not exist.
    """
    value = get_metric(metric, metadata)

    if value is None:
        return "NULL"
    else:
        return value


class FfprobeMetadata:
    """
    Class representing ffprobe metadata.

    Attributes:
        source_path (Path): The path to the source file.
        metadata (Dict[str, Any]): The ffprobe metadata.
        timestamp (datetime): The timestamp of when the metadata was retrieved.
        role (Optional[InterviewRole]): The role of the interviewee.
    """

    def __init__(
        self,
        source_path: Path,
        requested_by: str,
        metadata: Dict[str, Any],
        role: Optional[InterviewRole] = None,
    ):
        self.source_path: Path = source_path
        self.requested_by: str = requested_by
        self.metadata: Dict[str, Any] = metadata
        self.timestamp: datetime = datetime.now()
        self.role: Optional[InterviewRole] = role

    def __str__(self):
        return f"FfprobeMetadata({self.source_path}, {self.metadata}, {self.timestamp})"

    def __repr__(self):
        return self.__str__()

    @staticmethod
    def init_table_query() -> List[str]:
        """
        Returns a list of SQL queries to create the ffprobe_metadata tables.

        Returns:
            List[str]: A list of SQL queries.
        """
        metadata_table = """
        CREATE TABLE IF NOT EXISTS ffprobe_metadata (
            fm_source_path TEXT NOT NULL PRIMARY KEY,
            fm_requested_by TEXT NOT NULL,
            fm_format_name TEXT,
            fm_format_long_name TEXT,
            fm_duration TEXT,
            fm_size TEXT,
            fm_bit_rate TEXT,
            fm_probe_score TEXT,
            fm_tags TEXT,
            fm_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        """

        metadata_video_table = """
            CREATE TABLE ffprobe_metadata_video (
                fmv_source_path TEXT NOT NULL PRIMARY KEY REFERENCES ffprobe_metadata (fm_source_path),
                fmv_requested_by TEXT NOT NULL,
                ir_role VARCHAR(255),
                fmv_index INTEGER NOT NULL,
                fmv_codec_name VARCHAR(255) NOT NULL,
                fmv_codec_long_name VARCHAR(255) NOT NULL,
                fmv_profile VARCHAR(255) NOT NULL,
                fmv_codec_type VARCHAR(255) NOT NULL,
                fmv_codec_tag_string VARCHAR(255) NOT NULL,
                fmv_codec_tag VARCHAR(255) NOT NULL,
                fmv_width INTEGER NOT NULL,
                fmv_height INTEGER NOT NULL,
                fmv_coded_width INTEGER NOT NULL,
                fmv_coded_height INTEGER NOT NULL,
                fmv_closed_captions INTEGER NOT NULL,
                fmv_film_grain INTEGER NOT NULL,
                fmv_has_b_frames INTEGER NOT NULL,
                fmv_sample_aspect_ratio VARCHAR(255) NOT NULL,
                fmv_display_aspect_ratio VARCHAR(255) NOT NULL,
                fmv_pix_fmt VARCHAR(255) NOT NULL,
                fmv_level INTEGER NOT NULL,
                fmv_color_range VARCHAR(255) NOT NULL,
                fmv_chrorma_location VARCHAR(255) NOT NULL,
                fmv_field_order VARCHAR(255) NOT NULL,
                fmv_refs INTEGER NOT NULL,
                fmv_r_frame_rate VARCHAR(255) NOT NULL,
                fmv_avg_frame_rate VARCHAR(255) NOT NULL,
                fmv_time_base VARCHAR(255) NOT NULL,
                fmv_start_pts INTEGER NOT NULL,
                fmv_start_time VARCHAR(255) NOT NULL,
                fmv_duration VARCHAR(255) NOT NULL,
                fmv_extradata_size INTEGER NOT NULL
        );
        """

        metadata_audio_table = """
            CREATE TABLE ffprobe_metadata_audio (
                fma_source_path TEXT NOT NULL PRIMARY KEY REFERENCES ffprobe_metadata (fm_source_path),
                fma_requested_by TEXT NOT NULL,
                fma_index INTEGER NOT NULL,
                fma_codec_name VARCHAR(255) NOT NULL,
                fma_codec_long_name VARCHAR(255) NOT NULL,
                fma_profile VARCHAR(255) NOT NULL,
                fma_codec_type VARCHAR(255) NOT NULL,
                fma_codec_tag_string VARCHAR(255) NOT NULL,
                fma_codec_tag VARCHAR(255) NOT NULL,
                fma_sample_fmt VARCHAR(255) NOT NULL,
                fma_sample_rate INTEGER NOT NULL,
                fma_channels INTEGER NOT NULL,
                fma_channel_layout VARCHAR(255) NOT NULL,
                fma_bits_per_sample INTEGER NOT NULL,
                fma_r_frame_rate VARCHAR(255) NOT NULL,
                fma_avg_frame_rate VARCHAR(255) NOT NULL,
                fma_time_base VARCHAR(255) NOT NULL,
                fma_start_pts INTEGER NOT NULL,
                fma_start_time VARCHAR(255) NOT NULL,
                fma_duration VARCHAR(255) NOT NULL,
                fma_extradata_size INTEGER NOT NULL
            );
        """

        return [metadata_table, metadata_video_table, metadata_audio_table]

    @staticmethod
    def drop_table_query() -> List[str]:
        """
        Returns a list of SQL queries to drop the ffprobe_metadata tables.

        Returns:
            List[str]: A list of SQL queries.
        """
        drop_metadata_table = """
        DROP TABLE IF EXISTS ffprobe_metadata;
        """

        drop_metadata_video_table = """
            DROP TABLE IF EXISTS ffprobe_metadata_video;
        """

        drop_metadata_audio_table = """
            DROP TABLE IF EXISTS ffprobe_metadata_audio;
        """

        return [
            drop_metadata_video_table,
            drop_metadata_audio_table,
            drop_metadata_table,
        ]

    @staticmethod
    def stream_to_sql(
        stream: Dict[str, Any],
        source_path: Path,
        requested_by: str,
        role: Optional[InterviewRole] = None,
    ) -> str:
        """
        Convert a stream to a SQL query.

        Args:
            stream (Dict[str, Any]): The stream to convert.
            source_path (Path): The path to the source file.
            role (Optional[InterviewRole], optional): The role of the interviewee.
                Defaults to None.

        Returns:
            str: The SQL query.
        """
        if role is None:
            ir_role = "NULL"
        else:
            ir_role = role.value

        # Check if stream["duration"] exists
        if "duration" in stream:
            duration = stream["duration"]
        else:
            # check if stream["tags"]["DURATION"] exists
            if "DURATION" in stream["tags"]:
                duration = stream["tags"]["DURATION"]
            else:
                duration = 0

        if stream["codec_type"] == "video":
            query = f"""
                INSERT INTO ffprobe_metadata_video (
                    fmv_source_path,
                    fmv_requested_by,
                    ir_role,
                    fmv_index,
                    fmv_codec_name,
                    fmv_codec_long_name,
                    fmv_profile,
                    fmv_codec_type,
                    fmv_codec_tag_string,
                    fmv_codec_tag,
                    fmv_width,
                    fmv_height,
                    fmv_coded_width,
                    fmv_coded_height,
                    fmv_closed_captions,
                    fmv_film_grain,
                    fmv_has_b_frames,
                    fmv_sample_aspect_ratio,
                    fmv_display_aspect_ratio,
                    fmv_pix_fmt,
                    fmv_level,
                    fmv_color_range,
                    fmv_chrorma_location,
                    fmv_field_order,
                    fmv_refs,
                    fmv_r_frame_rate,
                    fmv_avg_frame_rate,
                    fmv_time_base,
                    fmv_start_pts,
                    fmv_start_time,
                    fmv_duration,
                    fmv_extradata_size
                ) VALUES (
                    '{source_path}',
                    '{requested_by}',
                    {ir_role},
                    {stream['index']},
                    '{stream['codec_name']}',
                    '{stream['codec_long_name']}',
                    '{stream['profile']}',
                    '{stream['codec_type']}',
                    '{stream['codec_tag_string']}',
                    '{stream['codec_tag']}',
                    {stream['width']},
                    {stream['height']},
                    {stream['coded_width']},
                    {stream['coded_height']},
                    {stream['closed_captions']},
                    {stream['film_grain']},
                    {stream['has_b_frames']},
                    '{metric_or_null('sample_aspect_ratio', stream)}',
                    '{metric_or_null('display_aspect_ratio', stream)}',
                    '{stream['pix_fmt']}',
                    {stream['level']},
                    '{metric_or_null('color_range', stream)}',
                    '{metric_or_null('chroma_location', stream)}',
                    '{stream['field_order']}',
                    {stream['refs']},
                    '{stream['r_frame_rate']}',
                    '{stream['avg_frame_rate']}',
                    '{stream['time_base']}',
                    {stream['start_pts']},
                    '{stream['start_time']}',
                    '{duration}',
                    {stream['extradata_size']}
                ) ON CONFLICT (fmv_source_path) DO NOTHING;
            """
        elif stream["codec_type"] == "audio":
            query = f"""
                INSERT INTO ffprobe_metadata_audio (
                    fma_source_path,
                    fma_requested_by,
                    fma_index,
                    fma_codec_name,
                    fma_codec_long_name,
                    fma_profile,
                    fma_codec_type,
                    fma_codec_tag_string,
                    fma_codec_tag,
                    fma_sample_fmt,
                    fma_sample_rate,
                    fma_channels,
                    fma_channel_layout,
                    fma_bits_per_sample,
                    fma_r_frame_rate,
                    fma_avg_frame_rate,
                    fma_time_base,
                    fma_start_pts,
                    fma_start_time,
                    fma_duration,
                    fma_extradata_size
                ) VALUES (
                    '{source_path}',
                    '{requested_by}',
                    {stream['index']},
                    '{stream['codec_name']}',
                    '{stream['codec_long_name']}',
                    '{stream['profile']}',
                    '{stream['codec_type']}',
                    '{stream['codec_tag_string']}',
                    '{stream['codec_tag']}',
                    '{stream['sample_fmt']}',
                    {stream['sample_rate']},
                    {stream['channels']},
                    '{stream['channel_layout']}',
                    {stream['bits_per_sample']},
                    '{stream['r_frame_rate']}',
                    '{stream['avg_frame_rate']}',
                    '{stream['time_base']}',
                    {stream['start_pts']},
                    '{stream['start_time']}',
                    '{duration}',
                    {stream['extradata_size']}
                ) ON CONFLICT (fma_source_path) DO NOTHING;
            """
        else:
            logger.warning(f"Unknown codec_type: {stream['codec_type']}")
            logger.info(f"Stream: {stream}")
            logger.warning("Skipping stream...")
            query = "SELECT 1;"  # No-op

        return query

    @staticmethod
    def drop_row_query(source_path: Path) -> List[str]:
        """
        Return the SQL queries to delete a row from the 'ffprobe_metadata' table.
        Also deletes the video and audio streams from the ffprobe_metadata_video and
        ffprobe_metadata_audio tables.

        Args:
            source_path (Path): Source path of the file.

        Returns:
            str: SQL query to delete the row.
        """

        queries = [
            f"""
            DELETE FROM ffprobe_metadata_video
            WHERE fmv_source_path = '{source_path}';
            """,
            f"""
            DELETE FROM ffprobe_metadata_audio
            WHERE fma_source_path = '{source_path}';
            """,
            f"""
            DELETE FROM ffprobe_metadata
            WHERE fm_source_path = '{source_path}';
            """,
        ]

        return queries

    def to_sql(self) -> List[str]:
        """
        Convert the ffprobe metadata to a list of SQL queries.
        - inserts the metadata into the ffprobe_metadata table.
        - inserts the video stream (if any) into the ffprobe_metadata_video table.
        - inserts the audio stream (if any) into the ffprobe_metadata_audio table.

        Returns:
            List[str]: A list of SQL queries.
        """
        try:
            streams = self.metadata["streams"]
        except KeyError as e:
            logger.error(f"Metadata does not have 'streams' key: {e}")
            logger.debug(f"Metadata: {self.metadata}")
            return [
                f"""
                INSERT INTO ffprobe_metadata (
                    fm_source_path,
                    fm_requested_by
                ) VALUES (
                    '{self.source_path}',
                    '{self.requested_by}'
                ) ON CONFLICT (fm_source_path) DO NOTHING;
                """
            ]
        format_dict: Dict[str, str] = self.metadata["format"]  # type: ignore

        sql_queries = []

        query = f"""
            INSERT INTO ffprobe_metadata (
                fm_source_path,
                fm_requested_by,
                fm_format_name,
                fm_format_long_name,
                fm_duration,
                fm_size,
                fm_bit_rate,
                fm_probe_score,
                fm_tags
            ) VALUES (
                '{self.source_path}',
                '{self.requested_by}',
                '{format_dict['format_name']}',
                '{format_dict['format_long_name']}',
                '{format_dict['duration']}',
                '{format_dict['size']}',
                '{format_dict['bit_rate']}',
                '{format_dict['probe_score']}',
                '{db.santize_string(str(format_dict['tags']))}'
            ) ON CONFLICT (fm_source_path) DO NOTHING;
        """

        sql_queries.append(query)

        for stream in streams:
            sql_queries.append(
                self.stream_to_sql(
                    stream=stream,
                    source_path=self.source_path,
                    role=self.role,
                    requested_by=self.requested_by,
                )
            )

        return sql_queries


if __name__ == "__main__":
    config_file = utils.get_config_file_path()

    console.log("Initializing 'ffprobe_metadata' table...")
    console.log(
        "[red]This will delete all existing data in the 'ffprobe_metadata' table![/red]"
    )

    drop_queries = FfprobeMetadata.drop_table_query()
    create_queries = FfprobeMetadata.init_table_query()

    sql_queries = drop_queries + create_queries

    db.execute_queries(config_file=config_file, queries=sql_queries)
    console.log("Done!")
