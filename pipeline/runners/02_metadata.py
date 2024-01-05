#!/usr/bin/env python

import sys
from pathlib import Path

file = Path(__file__).resolve()
parent = file.parent
root = None
for parent in file.parents:
    if parent.name == "av-pipeline-v2":
        root = parent
sys.path.append(str(root))

# remove current directory from path
try:
    sys.path.remove(str(parent))
except ValueError:
    pass

import logging
from typing import Optional, Dict

from rich.logging import RichHandler

from pipeline.helpers import utils, db, ffprobe
from pipeline import orchestrator, data
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

    return result


def log_metadata(source: Path, metadata: Dict, config_file: Path) -> None:
    ffprobe_metadata = FfprobeMetadata(
        source_path=source,
        metadata=metadata,
    )

    sql_queries = ffprobe_metadata.to_sql()

    logger.info("Logging metadata...", extra={"markup": True})
    db.execute_queries(config_file=config_file, queries=sql_queries)


if __name__ == "__main__":
    config_file = utils.get_config_file_path()
    utils.configure_logging(
        config_file=config_file, module_name=MODULE_NAME, logger=logger
    )

    console.rule(f"[bold red]{MODULE_NAME}")
    logger.info(f"Using config file: {config_file}")

    config_params = utils.config(config_file, section="general")
    study_id = config_params["study"]

    counter = 0

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
            if counter > 0:
                data.log(
                    config_file=config_file,
                    module_name=MODULE_NAME,
                    message=f"Gathered metadata for {counter} files.",
                )
                counter = 0

            # Snooze if no files to process
            orchestrator.snooze(config_file=config_file)
            continue

        counter += 1
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
