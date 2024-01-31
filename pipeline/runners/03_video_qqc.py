#!/usr/bin/env python
"""
Run Quick QC on video files
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
from typing import Optional, Tuple, List
import math
import tempfile

from rich.logging import RichHandler
import cv2

from pipeline.helpers import utils, db, ffmpeg
from pipeline.helpers.timer import Timer
from pipeline import orchestrator, data
from pipeline.models.video_qqc import VideoQuickQc

MODULE_NAME = "video-qqc"

logger = logging.getLogger(MODULE_NAME)
logargs = {
    "level": logging.DEBUG,
    # "format": "%(asctime)s - %(process)d - %(name)s - %(levelname)s - %(message)s",
    "format": "%(message)s",
    "handlers": [RichHandler(rich_tracebacks=True)],
}
logging.basicConfig(**logargs)

console = utils.get_console()


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
            FROM interview_files JOIN decrypted_files ON interview_files.interview_file = decrypted_files.source_path
        ) AS if
            ON fm.fm_source_path = if.destination_path
        WHERE fm.fm_source_path NOT IN (
            SELECT video_path FROM video_quick_qc
        ) AND fm.fm_source_path IN (
            SELECT destination_path FROM decrypted_files
            JOIN interview_files ON interview_files.interview_file = decrypted_files.source_path
            JOIN interviews USING (interview_path)
            WHERE interviews.study_id = '{study_id}'
        )
        ORDER BY RANDOM()
        LIMIT 1;
    """

    result_df = db.execute_sql(config_file=config_file, query=sql_query)
    if result_df.empty:
        return None

    video_path = result_df.iloc[0]["fm_source_path"]
    duration = result_df.iloc[0]["fm_duration"]

    return video_path, duration


def check_if_image_has_black_bars(
    image_file: Path, bars_height: float = 0.2, threshold: float = 0.8
) -> bool:
    """
    Checks if an image has black bars.

    Checks if top and bottom {threshold} of the image has black pixels, if majority of the
    pixels are black, then the image has black bars.

    Args:
        image_file (Path): Path to image file
        bars_height (float, optional): Height of the top and bottom bars. Defaults to 0.2.
            - 0.2 means 20% of the image height
        threshold (float, optional): Threshold to determine if the image has black bars.
            Defaults to 0.8.
            - If the ratio of black pixels to total pixels is greater than the threshold,
            then the image has black bars.

    Returns:
        bool: True if image has black bars, False otherwise
    """
    image = cv2.imread(str(image_file))
    height, width, _ = image.shape  # height, width, channels

    gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)

    # Get top and bottom bars
    bar_height = int(height * bars_height)
    top_pixels = gray[0:bar_height, 0:width]
    bottom_pixels = gray[height - bar_height: height, 0:width]

    pixels_count = width * bar_height

    top_black_pixels = pixels_count - cv2.countNonZero(top_pixels)
    bottom_black_pixels = pixels_count - cv2.countNonZero(bottom_pixels)

    total_black_pixels = top_black_pixels + bottom_black_pixels
    total_pixels = pixels_count * 2

    if total_black_pixels / total_pixels > threshold:
        return True
    else:
        return False


def get_black_bars_height(image_file: Path) -> float:
    """
    Gets the height of the black bars in the image.

    Args:
        image_file (Path): Path to image file
    """
    image = cv2.imread(str(image_file))
    gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)

    # apply threshold to invert the image
    _, thresh = cv2.threshold(gray, 3, 255, cv2.THRESH_BINARY_INV)

    # find contours of the white regions
    contours, _ = cv2.findContours(thresh, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)

    heights_y: List[Tuple[int, int]] = []
    y_heights: List[Tuple[int, int]] = []

    # loop over the contours
    for c in contours:
        # get the bounding rectangle of the contour
        _, y, _, h = cv2.boundingRect(c)  # x, y, w, h
        if h < 100:
            continue
        heights_y.append((h, y))
        y_heights.append((y, h))

    heights_y.sort(key=lambda x: x[0], reverse=False)
    y_heights.sort(key=lambda x: x[0], reverse=False)

    if y_heights[0][0] == 0:
        return y_heights[0][1]
    else:
        # average heights or 2 highest bars
        if len(heights_y) > 1:
            height = math.ceil((heights_y[0][0] + heights_y[1][0]) / 2)
        else:
            height = heights_y[0][0]
        return height


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
        has_black_bars = check_if_image_has_black_bars(image_file=screenshot)
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
        has_black_bars = check_if_image_has_black_bars(image_file=screenshot)
        if has_black_bars:
            black_bar_height = get_black_bars_height(image_file=screenshot)

            if black_bar_height > 200:
                black_bar_height = 180

            black_bar_heights.append(black_bar_height)

    # Median
    if len(black_bar_heights) > 0:
        return sorted(black_bar_heights)[len(black_bar_heights) // 2]
    else:
        return 0


def do_video_qqc(video_path: Path, duration: float) -> VideoQuickQc:
    """
    Performs video quick qc on a video file.

    - Extracts screenshots from the video
    - Checks if the video has black bars
    - Gets the black bar height

    Args:
        video_path (Path): Path to video file
        duration (float): Video duration
    """
    # Get screenshots
    num_screenshots = 10

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

    logger.info("[bold green]Starting video_qqc loop...", extra={"markup": True})

    while True:
        # Get file to process
        file_to_process = get_file_to_process(
            config_file=config_file, study_id=study_id
        )

        if file_to_process is None:
            # Log if any files were processed
            if COUNTER > 0:
                data.log(
                    config_file=config_file,
                    module_name=MODULE_NAME,
                    message=f"Checked video_qqc for {COUNTER} files.",
                )
                COUNTER = 0

            # Snooze if no files to process
            orchestrator.snooze(config_file=config_file)
            continue

        COUNTER += 1
        logger.info(
            f"[cyan]Checking video_qqc for {file_to_process}...",
            extra={"markup": True},
        )

        video_path = Path(file_to_process[0])
        duration = float(file_to_process[1])

        with Timer() as timer:
            qc_result: VideoQuickQc = do_video_qqc(
                video_path=video_path, duration=duration
            )

        # Add process time to qc_result
        qc_result.process_time = timer.duration

        log_video_qqc(
            config_file=config_file,
            result=qc_result,
        )
