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

import argparse
import logging
import tempfile
from typing import List, Optional, Tuple

from rich.logging import RichHandler

from pipeline import data, orchestrator
from pipeline.helpers import cli, db, ffmpeg, image, utils
from pipeline.helpers.timer import Timer
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
    parser = argparse.ArgumentParser(
        prog=MODULE_NAME, description="Run Quick QC on video files."
    )
    parser.add_argument(
        "-c", "--config", type=str, help="Path to the config file.", required=False
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

    COUNTER = 0

    logger.info("[bold green]Starting video_qqc loop...", extra={"markup": True})
    study_id = studies[0]
    logger.info(f"Using study: {study_id}")

    while True:
        # Get file to process
        file_to_process = get_file_to_process(
            config_file=config_file, study_id=study_id
        )

        if file_to_process is None:
            if study_id == studies[-1]:
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
                study_id = studies[0]
                logger.info(f"Restarting with study: {study_id}", extra={"markup": True})
                continue
            else:
                study_id = studies[studies.index(study_id) + 1]
                logger.info(f"Switching to study: {study_id}", extra={"markup": True})
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
