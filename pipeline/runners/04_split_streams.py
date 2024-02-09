#!/usr/bin/env python
"""
Split video streams into individual files

Splits Videos into
- Left - Participant and
- Right - Interviewer
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
from typing import List, Optional, Tuple

from rich.logging import RichHandler

from pipeline import data, orchestrator
from pipeline.helpers import cli, db, dpdash, ffmpeg, utils
from pipeline.helpers.timer import Timer
from pipeline.models.interview_roles import InterviewRole
from pipeline.models.video_streams import VideoStream

MODULE_NAME = "split-streams"

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
        INNER JOIN (
            SELECT decrypted_files.destination_path, interview_files.interview_file_tags
            FROM interview_files JOIN decrypted_files ON interview_files.interview_file = decrypted_files.source_path
        ) AS if
            ON vqqc.video_path = if.destination_path
        WHERE vqqc.video_path NOT IN (
            SELECT video_path FROM video_streams
        ) AND vqqc.video_path IN (
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
    left_crop_params = f"iw/2:ih-{2*black_bar_height}:0:{black_bar_height}"
    right_crop_params = f"iw/2:ih-{2*black_bar_height}:iw/2:{black_bar_height}"

    left_role = InterviewRole.from_str(config_params["left_role"])
    right_role = InterviewRole.from_str(config_params["right_role"])

    for role, crop_params in [
        (left_role, left_crop_params),
        (right_role, right_crop_params),
    ]:
        stream_file_path = construct_stream_path(
            video_path=video_path, role=role, suffix="mp4"
        )

        with Timer() as timer:
            ffmpeg.crop_video(
                source=video_path,
                target=stream_file_path,
                crop_params=crop_params,
                logger=logger,
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


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="decryption", description="Module to decrypt files."
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
    study_id = config_params["study"]

    COUNTER = 0
    STREAMS_COUNTER = 0

    logger.info("[bold green]Starting split streams loop...", extra={"markup": True})

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
                    message=f"Split {COUNTER} files into {STREAMS_COUNTER} streams.",
                )
                COUNTER = 0
                STREAMS_COUNTER = 0

            # Snooze if no files to process
            orchestrator.snooze(config_file=config_file)
            continue

        COUNTER += 1

        video_path = Path(file_to_process[0])
        has_black_bars = bool(file_to_process[1])
        black_bar_height = file_to_process[2]
        if black_bar_height is not None:
            black_bar_height = int(black_bar_height)

        logger.info(
            f"[cyan]Splitting streams for {video_path}...",
            extra={"markup": True},
        )
        logger.info(
            f"Black bars detected: {has_black_bars}",
            extra={"markup": True},
        )
        logger.info(
            f"Black bar height: {black_bar_height}",
            extra={"markup": True},
        )

        streams = split_streams(
            video_path=video_path,
            has_black_bars=has_black_bars,
            black_bar_height=black_bar_height,
            config_file=config_file,
        )
        STREAMS_COUNTER += len(streams)

        log_streams(config_file=config_file, streams=streams)
