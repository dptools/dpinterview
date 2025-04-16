#!/usr/bin/env python
"""
Remove all pipeline exported assets from the source directory.

Excludes assets tagged:
- acoustic_pipeline
- face_processing_pipeline
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
from typing import List

from rich.logging import RichHandler

from pipeline import orchestrator
from pipeline.helpers import cli, db, utils

MODULE_NAME = "wipe_exported_assets"

logger = logging.getLogger(MODULE_NAME)
logargs = {
    "level": logging.DEBUG,
    # "format": "%(asctime)s - %(process)d - %(name)s - %(levelname)s - %(message)s",
    "format": "%(message)s",
    "handlers": [RichHandler(rich_tracebacks=True)],
}
logging.basicConfig(**logargs)

console = utils.get_console()


def get_items_to_delete(
    config_file: Path,
    study_id: str,
    excluded_tags: List[str],
) -> List[Path]:
    """
    Get a list of files or directories to delete from the source directory.

    Args:
        config_file (Path): The path to the config file.
        study_id (str): The study ID.
        excluded_tags (List[str]): A list of tags to exclude.

    Returns:
        List[Path]: A list of files to delete.
    """
    excluded_tags = [f"'{tag}'" for tag in excluded_tags]
    excluded_tags_str = ", ".join(excluded_tags)

    query = f"""
    SELECT *
    FROM exported_assets
    LEFT JOIN interviews USING (interview_name)
    WHERE study_id = '{study_id}' AND
        asset_tag NOT IN ({excluded_tags_str})
    """

    df = db.execute_sql(config_file=config_file, query=query)

    logger.debug(f"Found {len(df)} files to delete.")

    delete_path = df["asset_destination"].tolist()
    delete_path = [Path(row) for row in delete_path]

    return delete_path


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

    studies = orchestrator.get_studies(config_file=config_file)

    COUNTER = 0
    SKIP_COUNTER = 0
    for study_id in studies:
        logger.info(f"Starting with study: {study_id}", extra={"markup": True})

        items_to_delete = get_items_to_delete(
            config_file=config_file,
            study_id=study_id,
            excluded_tags=["acoustic_pipeline", "face_processing_pipeline"],
        )

        for item in items_to_delete:
            try:
                cli.remove(item)
                COUNTER += 1
            except FileNotFoundError:
                SKIP_COUNTER += 1
                continue

    logger.info(f"Deleted {COUNTER} items.")
    logger.info(f"Skipped {SKIP_COUNTER} items.")
    logger.info("Done!")
