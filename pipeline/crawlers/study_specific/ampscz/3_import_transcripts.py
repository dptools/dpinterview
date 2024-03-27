#!/usr/bin/env python
"""
Walks through the transcripts folder and imports the transcripts into the database.
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
import argparse
from typing import List, Tuple

from rich.logging import RichHandler

from pipeline import core, orchestrator
from pipeline.core.fetch_video import check_if_interview_has_duplicates
from pipeline.helpers import cli, utils, db
from pipeline.models.files import File
from pipeline.models.interview_files import InterviewFile

MODULE_NAME = "import_transcript_files"
INSTANCE_NAME = MODULE_NAME

logger = logging.getLogger(MODULE_NAME)
logargs = {
    "level": logging.DEBUG,
    # "format": "%(asctime)s - %(process)d - %(name)s - %(levelname)s - %(message)s",
    "format": "%(message)s",
    "handlers": [RichHandler(rich_tracebacks=True)],
}
logging.basicConfig(**logargs)

console = utils.get_console()


def get_interview_name_from_transcript(transcript_filename: str) -> str:
    """
    Maps the transcript filename to the interview name.

    Note: Adjusts the day number by 1 to match the day number in the interview name.

    Args:
        transcript_filename (str): The filename of the transcript.

    Returns:
        str: The interview name.
    """
    name_parts = transcript_filename.split("_")
    study_id = name_parts[0]
    subject_id = name_parts[1]
    data_type = name_parts[3]
    day_str = name_parts[4]  # dayXXXX
    day = int(day_str[3:])

    interview_name = f"{study_id}-{subject_id}-{data_type}Interview-day{day:04d}"

    return interview_name


def transcripts_to_models(
    transcripts: List[Path], config_file: Path
) -> Tuple[List[File], List[InterviewFile]]:
    """
    Converts the transcripts into File and InterviewFile models.

    Args:
        transcripts (List[Path]): The list of transcripts.

    Returns:
        Tuple[List[File], List[InterviewFile]]: The list of File and InterviewFile models.
    """
    files = []
    interview_files = []

    for transcript in transcripts:
        filename = transcript.name

        interview_name = get_interview_name_from_transcript(
            transcript_filename=filename
        )

        if check_if_interview_has_duplicates(
            interview_name=interview_name, config_file=config_file
        ):
            logger.warning(
                f"Interview {interview_name} has duplicates. Skipping transcript."
            )
            continue

        interview_path = core.get_interview_path(
            interview_name=interview_name, config_file=config_file
        )

        if interview_path is None:
            logger.warning(f"Interview path not found for {interview_name}. Skipping.")
            continue

        file = File(file_path=transcript)
        i_file = InterviewFile(
            interview_path=interview_path,
            interview_file=transcript,
            tags="transcript,transcribeme",
        )

        files.append(file)
        interview_files.append(i_file)

    return files, interview_files


def models_to_db(
    files: List[File], interview_files: List[InterviewFile], config_file: Path
) -> None:
    """
    Imports the File and InterviewFile models into the database.

    Args:
        files (List[File]): The list of File models.
        interview_files (List[InterviewFile]): The list of InterviewFile models.

    Returns:
        None
    """

    sql_queries = []

    for file in files:
        sql_queries.append(file.to_sql())

    for i_file in interview_files:
        sql_queries.append(i_file.to_sql())

    db.execute_queries(queries=sql_queries, config_file=config_file, show_commands=False)


def import_transcripts(data_root: Path, study: str, config_file: Path) -> None:
    """
    Walks through the transcripts folder and imports the transcripts into the database.

    Args:
        data_root (Path): The root path of the data.
        study (str): The study name.

    Returns:
        None
    """
    subjects_root = data_root / "PROTECTED" / study

    crawler_params = utils.config(path=config_file, section="crawler")
    transcripts_study_pattern = crawler_params["transcripts_study_pattern"]

    transcripts = list(subjects_root.glob(transcripts_study_pattern))

    logger.info(f"Found {len(transcripts)} transcripts.")

    files, interview_files = transcripts_to_models(
        transcripts=transcripts, config_file=config_file
    )

    logger.info(
        f"Importing {len(files)} files and {len(interview_files)} interview files."
    )
    models_to_db(files=files, interview_files=interview_files, config_file=config_file)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog=MODULE_NAME, description="Import transcripts into the database."
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

    data_root = orchestrator.get_data_root(config_file=config_file, enforce_real=True)
    studies = orchestrator.get_studies(config_file=config_file)

    for study in studies:
        logger.info(f"Processing study: {study}")
        import_transcripts(data_root=data_root, study=study, config_file=config_file)

    logger.info("[bold green]Done!", extra={"markup": True})
