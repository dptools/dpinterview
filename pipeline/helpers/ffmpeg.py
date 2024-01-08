from pathlib import Path
from typing import List, Optional
import logging

from pipeline.helpers import utils, cli


def extract_screenshots_from_video(
    video_file: Path,
    video_duration: float,
    output_dir: Path,
    num_screenshots: int = 10,
    logger: Optional[logging.Logger] = None,
) -> List[Path]:
    """
    Extracts screenshots from a video file using ffmpeg.

    WARNING: This function will delete all files in the output_dir.

    Args:
        video_file (Path): The path to the video file.
        video_duration (float): The duration of the video in seconds.
        output_dir (Path): The directory where the screenshots will be saved.
        num_screenshots (int, optional): The number of screenshots to extract. Defaults to 10.

    Returns:
        List[Path]: A list of paths to the extracted screenshots.
    """
    if logger is None:
        logger = logging.getLogger(__name__)

    # Extract screenshots from video using ffmpeg
    logger.info("[green]Extracting frames from video...", extra={"markup": True})

    # Check if output_dir exists
    if not output_dir.exists():
        logger.debug("[green]Creating output_dir...", extra={"markup": True})
        output_dir.mkdir(parents=True, exist_ok=True)

    # Check if output_path is empty
    if len(list(output_dir.iterdir())) > 0:
        logger.debug("[red]Clearing output_path...", extra={"markup": True})
        for file in output_dir.iterdir():
            file.unlink()

    # Get interval between screenshots
    interval = video_duration / num_screenshots

    # Extract screenshots
    extension: str = "png"
    command_array = [
        "ffmpeg",
        "-i",
        str(video_file),
        "-vf",
        f"fps=1/{interval}",
        "-qscale:v",
        "2",
        f"{str(output_dir)}/%d.{extension}",
        "-y",  # overwrite existing files
    ]

    with utils.get_progress_bar() as progress:
        progress.add_task("[green]Extracting frames from video...", total=None)
        cli.execute_commands(command_array)

    # Get list of screenshots
    screenshots = list(output_dir.glob(f"*.{extension}"))

    return screenshots
