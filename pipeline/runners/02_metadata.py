#!/usr/bin/env python
"""
Gather metadata for files
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
from typing import Dict, Optional

from rich.logging import RichHandler

from pipeline import data, orchestrator
from pipeline.helpers import cli, db, ffprobe, utils
from pipeline.models.ffprobe_metadata import FfprobeMetadata

MODULE_NAME = "metadata"

logger = logging.getLogger(MODULE_NAME)
logargs = {
    "level": logging.DEBUG,
    # "format": "%(asctime)s - %(process)d - %(name)s - %(levelname)s - %(message)s",
    "format": "%(message)s",
    "handlers": [RichHandler(rich_tracebacks=True)],
}
logging.basicConfig(**logargs)

console = utils.get_console()


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
            FROM interview_files JOIN interviews USING (interview_path)
            WHERE study_id = '{study_id}'
        )
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
                SELECT destination_path FROM decrypted_files
                JOIN interview_files ON interview_files.interview_file = decrypted_files.source_path
                JOIN interviews USING (interview_path)
                WHERE interviews.study_id = '{study_id}'
            )
            ORDER BY RANDOM()
            LIMIT 1;
        """

        result = db.fetch_record(config_file=config_file, query=sql_query)

    return result


def log_metadata(source: Path, metadata: Dict, config_file: Path) -> None:
    """
    Logs metadata to the database.

    Args:
        source (Path): Path to source file
        metadata (Dict): Metadata to log
        config_file (Path): Path to config file
    """
    ffprobe_metadata = FfprobeMetadata(
        source_path=source,
        metadata=metadata,
    )

    sql_queries = ffprobe_metadata.to_sql()

    logger.info("Logging metadata...", extra={"markup": True})
    db.execute_queries(config_file=config_file, queries=sql_queries)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog=MODULE_NAME, description="Gather metadata for files."
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

    logger.info(
        "[bold green]Starting metadata gathering loop...", extra={"markup": True}
    )
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
                    message=f"Gathered metadata for {COUNTER} files.",
                )
                COUNTER = 0

            # Snooze if no files to process
            orchestrator.snooze(config_file=config_file)
            continue

        COUNTER += 1
        logger.info(
            f"[cyan] Getting Metadata for{file_to_process}...", extra={"markup": True}
        )

        metadata = ffprobe.get_metadata(
            file_path_to_process=Path(file_to_process), config_file=config_file
        )

        log_metadata(
            source=Path(file_to_process),
            metadata=metadata,
            config_file=config_file,
        )
