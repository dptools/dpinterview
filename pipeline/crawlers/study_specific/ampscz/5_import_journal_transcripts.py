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
    if parent.name == "dpinterview":
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

from pipeline import orchestrator
from pipeline.helpers import cli, utils, db
from pipeline.models.files import File
from pipeline.models.transcript_files import TranscriptFile

MODULE_NAME = "import_journal_transcript_files"
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


def get_diary_name_from_transcript(transcript_filename: str) -> str:
    """
    Maps the transcript filename to the audio journal name

    Args:
        transcript_filename (str): The filename of the transcript.

    Returns:
        str: The name of the audio journal.
    """
    # Remove extension
    transcript_filename = transcript_filename.split(".")[0]

    name_parts = transcript_filename.split("_")
    try:
        study_id = name_parts[0]
        subject_id = name_parts[1]
        data_type = name_parts[2]
        day_str = name_parts[3]  # dayXXXX
        day = int(day_str[3:])
        session_str = name_parts[4]  # submissionXXXX
        session = int(session_str[10:])
    except Exception as e:
        logger.error(f"Error processing transcript {transcript_filename}: {e}")
        raise e

    # make all day as positive
    if day < 0:
        day = -day
    # make all session as positive
    if session < 0:
        session = -session

    journal_name = f"{study_id}-{subject_id}-{data_type}-day{day:04d}-session{session:04d}"

    return journal_name


def transcripts_to_models(
    transcripts: List[Path]
) -> Tuple[List[File], List[TranscriptFile]]:
    """
    Converts the transcripts into File and InterviewFile models.

    Args:
        transcripts (List[Path]): The list of transcripts.

    Returns:
        Tuple[List[File], List[TranscriptFile]]: The list of File and InterviewFile models.
    """
    files: List[File] = []
    transcript_files: List[TranscriptFile] = []

    with utils.get_progress_bar() as progress:
        task = progress.add_task("Processing transcripts", total=len(transcripts))
        for transcript in transcripts:
            filename = transcript.name

            try:
                audio_jounal_name = get_diary_name_from_transcript(
                    transcript_filename=filename
                )
            except IndexError as e:
                logger.error(f"Error processing transcript {filename}: {e}")
                logger.error("Skipping.")
                continue

            file = File(file_path=transcript)
            t_file = TranscriptFile(
                transcript_file=transcript,
                identifier_name=audio_jounal_name,
                identifier_type="audioJounal",
                tags="transcribeme",
            )

            files.append(file)
            transcript_files.append(t_file)
            progress.update(task, advance=1)

    return files, transcript_files


def models_to_db(
    files: List[File], transcript_files: List[TranscriptFile], config_file: Path
) -> None:
    """
    Imports the File and InterviewFile models into the database.

    Args:
        files (List[File]): The list of File models.
        transcript_files (List[InterviewFile]): The list of InterviewFile models.

    Returns:
        None
    """

    sql_queries = []

    for file in files:
        sql_queries.append(file.to_sql())

    for t_file in transcript_files:
        sql_queries.append(t_file.to_sql())

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
    transcripts_study_patterns = crawler_params["journal_transcripts_study_pattern"]
    transcripts_study_patterns = transcripts_study_patterns.split(",")
    transcripts_study_patterns = [x.strip() for x in transcripts_study_patterns]

    transcripts: List[Path] = []

    for transcripts_study_pattern in transcripts_study_patterns:
        transcripts_list = list(subjects_root.glob(transcripts_study_pattern))
        transcripts.extend(transcripts_list)

    logger.info(f"Found {len(transcripts)} transcripts.")

    files, transcript_files = transcripts_to_models(
        transcripts=transcripts
    )

    logger.info(
        f"Importing {len(files)} files and {len(transcript_files)} transcript files."
    )
    models_to_db(files=files, transcript_files=transcript_files, config_file=config_file)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog=MODULE_NAME, description="Import audio journal transcripts into the database."
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
