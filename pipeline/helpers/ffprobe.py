#!/usr/bin/env python
"""
Helper funtions for interacting with the ffprobe
"""

import sys
from pathlib import Path

file = Path(__file__).resolve()
ROOT = None
parent = file.parent
for parent in file.parents:
    if parent.name == "pipeline":
        ROOT = parent
sys.path.append(str(ROOT))

# remove current directory from path
try:
    sys.path.remove(str(parent))
except ValueError:
    pass

# Reference:
# https://stackoverflow.com/questions/9896644/getting-ffprobe-information-with-python

import argparse
import json
import shlex
import shutil
import subprocess
from datetime import date, datetime, timezone
from typing import NamedTuple, Optional
import logging

from pipeline.helpers import cli
from pipeline.helpers.config import config

logger = logging.getLogger(__name__)

FFMPEG_LIB_BIN_PATH = None
FFPROBE_BIN_PATH = 'ffprobe'  # default to system ffprobe


def set_ffmpeg_bin_path(path: str) -> None:
    """
    Set the FFmpeg binary path.

    Args:
        path (str): The path to the FFmpeg binary.
    """
    global FFMPEG_LIB_BIN_PATH  # pylint: disable=global-statement
    global FFPROBE_BIN_PATH  # pylint: disable=global-statement
    FFMPEG_LIB_BIN_PATH = path
    FFPROBE_BIN_PATH = str(Path(FFMPEG_LIB_BIN_PATH) / "ffprobe")
    logger.info(f"FFmpeg library path set to: {FFMPEG_LIB_BIN_PATH}")


class FFProbeResult(NamedTuple):
    """
    Represents the result of an FFProbe command.

    Attributes:
        return_code (int): The return code of the FFProbe command.
        json (str): The JSON output of the FFProbe command.
        error (str): Any error message produced by the FFProbe command.
    """

    return_code: int
    json: str
    error: str


def can_probe(config_file: Optional[Path]) -> bool:
    """
    Checks whether ffprobe can run, through the binary path or the singularity image.

    Args:
        config_file (Optional[Path]): The configuration file; None means the system ffprobe.

    Returns:
        bool: False where ffprobe() would fail or cli.singularity_run() would exit.
    """
    if config_file is None or FFMPEG_LIB_BIN_PATH:
        return shutil.which(FFPROBE_BIN_PATH) is not None
    try:
        params = config(path=config_file, section="singularity")
    except ValueError:
        return False
    binary = params.get("singularity_binary_path", "singularity")
    image = params.get("singularity_image_path")
    return (
        shutil.which(binary) is not None
        and image is not None
        and Path(image).is_file()
    )


def get_creation_date(config_file: Optional[Path], file_path: Path) -> Optional[date]:
    """
    Reads the UTC day of a media file's container creation_time tag. Never raises.

    Args:
        config_file (Optional[Path]): The configuration file, for the singularity image.
        file_path (Path): The media file.

    Returns:
        Optional[date]: The day, or None if ffprobe is unavailable, fails, or finds no tag.
    """
    if not can_probe(config_file):
        return None
    try:
        result = ffprobe(file_path=file_path, config_file=config_file)
        tags = json.loads(result.json).get("format", {}).get("tags", {})
        creation_time = tags.get("creation_time")
        if result.return_code != 0 or not creation_time:
            return None
        created = datetime.fromisoformat(creation_time.replace("Z", "+00:00"))
    except (ValueError, OSError) as e:
        logger.warning(f"Could not read a creation date from {file_path}: {e}")
        return None

    if created.tzinfo is None:
        created = created.replace(tzinfo=timezone.utc)
    return created.astimezone(timezone.utc).date()


def get_metadata(config_file: Optional[Path], file_path_to_process: Path) -> dict:
    """
    Retrieves metadata from a file using ffprobe.

    Args:
        config_file (Path): The path to the configuration file.
            - contains singularity image path
        file_path_to_process (Path): The path to the file to retrieve metadata from.

    Returns:
        dict: A dictionary containing the metadata retrieved from the file.
    """
    ffprobe_result = ffprobe(file_path_to_process, config_file=config_file)

    if ffprobe_result.return_code != 0:
        print("Error: ffprobe failed.")
        print(ffprobe_result.error, file=sys.stderr)

    metadata = json.loads(ffprobe_result.json)

    return metadata


def ffprobe(file_path: Path, config_file: Optional[Path] = None) -> FFProbeResult:
    """
    Runs ffprobe on a file and returns the result.

    Args:
        file_path (Path): The path to the file to run ffprobe on.
        config_file (Path, optional): The path to the configuration file. Defaults to None.

    Returns:
        FFProbeResult: The result of the ffprobe command.
    """

    if config_file is not None and not FFMPEG_LIB_BIN_PATH:
        command_array = [
            "ffprobe",
            "-v",
            "quiet",
            "-print_format",
            "json",
            "-show_format",
            "-show_streams",
            shlex.quote(str(file_path)),
        ]
        command_array = cli.singularity_run(
            config_file=config_file, command_array=command_array
        )
    else:
        command_array = [
            FFPROBE_BIN_PATH,
            "-v",
            "quiet",
            "-print_format",
            "json",
            "-show_format",
            "-show_streams",
            shlex.quote(str(file_path)),
        ]

    logger = logging.getLogger(__name__)
    logger.debug(f"Running ffprobe command: {' '.join(command_array)}")

    result = subprocess.run(
        " ".join(command_array),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True,
        shell=True,
        check=False,
    )

    return FFProbeResult(
        return_code=result.returncode, json=result.stdout, error=result.stderr
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="View ffprobe output")
    parser.add_argument("-i", "--input", help="File Name", required=True)
    args = parser.parse_args()
    if not Path(args.input).is_file():
        print("could not read file: " + args.input)
        exit(1)
    print(f"File:       {args.input}")
    ffprobe_result = ffprobe(file_path=args.input)
    if ffprobe_result.return_code == 0:
        # Print the raw json string
        print(ffprobe_result.json)

        # or print a summary of each stream
        d = json.loads(ffprobe_result.json)
        streams = d.get("streams", [])
        for stream in streams:
            print(
                f'{stream.get("codec_type", "unknown")}: {stream.get("codec_long_name")}'
            )

    else:
        print("ERROR")
        print(ffprobe_result.error, file=sys.stderr)
