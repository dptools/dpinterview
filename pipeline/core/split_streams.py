"""
Helper functions to split video into streams.
"""

import logging
from pathlib import Path
from typing import List, Optional, Tuple

from pipeline import orchestrator
from pipeline.helpers import db, dpdash, ffmpeg, utils
from pipeline.helpers.timer import Timer
from pipeline.models.interview_roles import InterviewRole
from pipeline.models.video_streams import VideoStream

logger = logging.getLogger(__name__)


def get_file_to_process(
    config_file: Path, study_id: str
) -> Optional[Tuple[Path, bool, int]]:
    """
    Fetch a file to process from the database.

    - Fetches a file that has not been processed yet and is part of the study.
        - Must have metadata extracted

    Args:
        config_file (Path): Path to config file
    """
    sql_query = f"""
        SELECT vqqc.video_path, vqqc.has_black_bars, vqqc.black_bar_height
        FROM video_quick_qc AS vqqc
        LEFT JOIN (
            SELECT decrypted_files.destination_path, interview_files.interview_file_tags
            FROM interview_files
            LEFT JOIN decrypted_files ON interview_files.interview_file = decrypted_files.source_path
        ) AS if
            ON vqqc.video_path = if.destination_path
        WHERE vqqc.video_path NOT IN (
            SELECT video_path
            FROM video_streams
        ) AND vqqc.video_path IN (
            SELECT destination_path
            FROM decrypted_files
            LEFT JOIN interview_files ON interview_files.interview_file = decrypted_files.source_path
            LEFT JOIN interview_parts USING (interview_path)
            LEFT JOIN interviews USING (interview_name)
            WHERE interviews.study_id = '{study_id}'
        )
        ORDER BY RANDOM()
        LIMIT 1;
    """

    result_df = db.execute_sql(config_file=config_file, query=sql_query)
    if result_df.empty:
        return None

    video_path = Path(result_df.iloc[0]["video_path"])
    has_black_bars = bool(result_df.iloc[0]["has_black_bars"])
    black_bar_height = result_df.iloc[0]["black_bar_height"]
    if black_bar_height is not None:
        black_bar_height = int(black_bar_height)

    return video_path, has_black_bars, black_bar_height


def construct_stream_path(video_path: Path, role: InterviewRole, suffix: str) -> Path:
    """
    Constructs a dpdash compliant stream path

    Args:
        video_path (Path): Path to video
        role (InterviewRole): Role of the stream
        suffix (str): Suffix of the stream
    """
    dpdash_dict = dpdash.parse_dpdash_name(video_path.name)
    if dpdash_dict["optional_tags"] is None:
        optional_tag: List[str] = []
    else:
        optional_tag = dpdash_dict["optional_tags"]  # type: ignore

    optional_tag.append(role.value)
    dpdash_dict["optional_tags"] = optional_tag

    dpdash_dict["category"] = "video"

    dp_dash_name = dpdash.get_dpdash_name_from_dict(dpdash_dict)
    stream_path = video_path.parent / "streams" / f"{dp_dash_name}.{suffix}"

    # Create streams directory if it doesn't exist
    stream_path.parent.mkdir(parents=True, exist_ok=True)

    return stream_path


def split_streams(
    video_path: Path,
    has_black_bars: bool,
    black_bar_height: Optional[int],
    config_file: Path,
) -> List[VideoStream]:
    """
    Split video into streams

    Args:
        video_path (Path): Path to video
        has_black_bars (bool): Whether video has black bars
        black_bar_height (Optional[int]): Height of black bars
        config_file (Path): Path to config file
    """
    config_params = utils.config(path=config_file, section="split-streams")
    default_role = InterviewRole.from_str(config_params["default_role"])

    streams = []
    logger.info(f"Splitting streams for {video_path}...", extra={"markup": True})

    if not has_black_bars:
        logger.info("No black bars detected. Skipping splitting streams.")
        stream: VideoStream = VideoStream(
            video_path=video_path, ir_role=default_role, vs_path=video_path
        )
        streams.append(stream)
        return streams

    if black_bar_height is None:
        black_bar_height = 0

    # out_w:out_h:x:y
    left_crop_params = f"iw/2:ih-{2 * black_bar_height}:0:{black_bar_height}"
    right_crop_params = f"iw/2:ih-{2 * black_bar_height}:iw/2:{black_bar_height}"

    left_role = InterviewRole.from_str(config_params["left_role"])
    right_role = InterviewRole.from_str(config_params["right_role"])

    with utils.get_progress_bar() as progress:
        task = progress.add_task("Splitting streams", total=2)
        for role, crop_params in [
            (left_role, left_crop_params),
            (right_role, right_crop_params),
        ]:
            progress.update(task, description=f"Splitting {role.value} stream")
            stream_file_path = construct_stream_path(
                video_path=video_path, role=role, suffix="mp4"
            )

            with Timer() as timer:
                ffmpeg.crop_video(
                    source=video_path,
                    target=stream_file_path,
                    crop_params=crop_params,
                    progress=progress,
                )
                orchestrator.fix_permissions(
                    config_file=config_file, file_path=stream_file_path
                )

            logger.info(
                f"Split {role.value} stream: {stream_file_path} ({timer.duration})",
                extra={"markup": True},
            )

            stream: VideoStream = VideoStream(
                video_path=video_path,
                ir_role=role,
                vs_path=stream_file_path,
                vs_process_time=timer.duration,
            )
            streams.append(stream)
            progress.update(task, advance=1)

    return streams


def log_streams(config_file: Path, streams: List[VideoStream]) -> None:
    """
    Log streams to database.

    Args:
        config_file (Path): Path to config file
        streams (List[VideoStream]): List of streams
    """
    sql_queries = [stream.to_sql() for stream in streams]

    logger.info("Inserting streams into DB", extra={"markup": True})
    db.execute_queries(config_file=config_file, queries=sql_queries)
