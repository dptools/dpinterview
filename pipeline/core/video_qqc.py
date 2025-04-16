"""
Helper functions for video quick qc.
"""

import logging
import tempfile
from pathlib import Path
from typing import List, Optional, Tuple

from pipeline.helpers import db, ffmpeg, image
from pipeline.models.video_qqc import VideoQuickQc

logger = logging.getLogger(__name__)


def get_file_to_process(
    config_file: Path, study_id: str
) -> Optional[Tuple[str, float]]:
    """
    Fetch a file to process from the database, that has not been processed yet.

    - Fetches a file that has not been processed yet
    - Fetches a file that is part of the study

    Args:
        config_file (Path): Path to config file
        study_id (str): Study ID
    """
    sql_query = f"""
        SELECT fm.fm_source_path, fm.fm_duration
        FROM ffprobe_metadata AS fm
        INNER JOIN (
            SELECT decrypted_files.destination_path, interview_files.interview_file_tags
            FROM interview_files
            LEFT JOIN decrypted_files ON interview_files.interview_file = decrypted_files.source_path
        ) AS if
            ON fm.fm_source_path = if.destination_path
        WHERE fm.fm_source_path NOT IN (
            SELECT video_path FROM video_quick_qc
        ) AND fm.fm_source_path IN (
            SELECT destination_path
            FROM decrypted_files
            LEFT JOIN interview_files ON interview_files.interview_file = decrypted_files.source_path
            LEFT JOIN interview_parts USING (interview_path)
            JOIN interviews USING (interview_name)
            WHERE interviews.study_id = '{study_id}'
        ) AND fm.fm_duration IS NOT NULL
        ORDER BY RANDOM()
        LIMIT 1;
    """

    result_df = db.execute_sql(config_file=config_file, query=sql_query)
    if result_df.empty:
        return None

    video_path = result_df.iloc[0]["fm_source_path"]
    duration = result_df.iloc[0]["fm_duration"]

    return video_path, duration


def sanitize_black_bar_height(height: float) -> float:
    """
    Caps the black bar height at 180px

    - This is to avoid the case where the black bar height is the height of the video

    Args:
        height (float): Black bar height
    """
    if height > 200:
        return 180

    return height


def check_black_bars(screenshots: List[Path]) -> bool:
    """
    Checks is a majority of the screenshots have black bars.

    Args:
        screenshots (List[Path]): List of screenshots
    """
    black_bar_count = 0

    for screenshot in screenshots:
        has_black_bars = image.check_if_image_has_black_bars(image_file=screenshot)
        if has_black_bars:
            black_bar_count += 1

    if black_bar_count > 0.5 * len(screenshots):
        return True
    else:
        return False


def get_black_bar_height(screenshots: List[Path]) -> int:
    """
    Gets the median black bar height from the screenshots.

    Args:
        screenshots (List[Path]): List of screenshots
    """
    black_bar_heights = []

    for screenshot in screenshots:
        has_black_bars = image.check_if_image_has_black_bars(image_file=screenshot)
        if has_black_bars:
            black_bar_height = image.get_black_bars_height(image_file=screenshot)

            if black_bar_height > 200:
                black_bar_height = 180

            black_bar_heights.append(black_bar_height)

    # Median
    if len(black_bar_heights) > 0:
        return sorted(black_bar_heights)[len(black_bar_heights) // 2]
    else:
        return 0


def do_video_qqc(
    video_path: Path,
    duration: float,
    frames_path: Optional[Path] = None
) -> VideoQuickQc:
    """
    Performs video quick qc on a video file.

    - Extracts screenshots from the video
    - Checks if the video has black bars
    - Gets the black bar height

    Args:
        video_path (Path): Path to video file
        duration (float): Video duration
        frames_path (Optional[Path], optional): Path to store extracted frames. Defaults to None.
    """
    # Get screenshots
    num_screenshots = 10

    if frames_path is None:
        with tempfile.TemporaryDirectory(prefix="video-qqc-") as temp_dir:
            screenshots = ffmpeg.extract_screenshots_from_video(
                video_file=video_path,
                video_duration=duration,
                output_dir=Path(temp_dir),
                num_screenshots=num_screenshots,
            )
            # Check if video has black bars
            has_black_bars = check_black_bars(screenshots=screenshots)
            if not has_black_bars:
                return VideoQuickQc(
                    video_path=video_path,
                    has_black_bars=False,
                    black_bar_height=None,
                    process_time=None,
                )
            else:
                black_bar_height = get_black_bar_height(screenshots=screenshots)
                return VideoQuickQc(
                    video_path=video_path,
                    has_black_bars=True,
                    black_bar_height=black_bar_height,
                    process_time=None,
                )
    else:
        screenshots = ffmpeg.extract_screenshots_from_video(
            video_file=video_path,
            video_duration=duration,
            output_dir=frames_path,
            num_screenshots=num_screenshots,
        )
        # Check if video has black bars
        has_black_bars = check_black_bars(screenshots=screenshots)
        if not has_black_bars:
            return VideoQuickQc(
                video_path=video_path,
                has_black_bars=False,
                black_bar_height=None,
                process_time=None,
            )
        else:
            black_bar_height = get_black_bar_height(screenshots=screenshots)
            return VideoQuickQc(
                video_path=video_path,
                has_black_bars=True,
                black_bar_height=black_bar_height,
                process_time=None,
            )


def log_video_qqc(
    config_file: Path,
    result: VideoQuickQc,
) -> None:
    """
    Logs the video_qqc result to the database.

    Args:
        config_file (Path): Path to config file
        result (VideoQuickQc): VideoQuickQc result
    """
    sql_query = result.to_sql()

    logger.info("Logging video_qqc...", extra={"markup": True})
    db.execute_queries(config_file=config_file, queries=[sql_query])
