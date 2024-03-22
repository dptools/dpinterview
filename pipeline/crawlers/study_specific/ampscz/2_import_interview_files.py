#!/usr/bin/env python
"""
Walks through interview directories and imports the interview files into the database.

The interview files are categorized into the following types:
- Combined audio and video
- Participant audio
- Interviewer audio

The files are then inserted into the database.
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
import multiprocessing
import re
from datetime import date, datetime, time
from typing import Dict, List

from rich.logging import RichHandler
from rich.progress import Progress

from pipeline import core, orchestrator
from pipeline.helpers import cli, db, dpdash, utils
from pipeline.helpers.config import config
from pipeline.models.files import File
from pipeline.models.interview_files import InterviewFile
from pipeline.models.interviews import Interview, InterviewType

MODULE_NAME = "import_interview_files"
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


def catogorize_audio_files(
    audio_files: List[Path], subject_id: str
) -> Dict[str, List[Path]]:
    """
    Categorizes the audio files into participant, interviewer, and combined audio files.

    Determines the type of audio file based on the file name and the subject ID.
    - If the file name contains the subject ID, it is categorized as a participant audio file.
    - If the file name contains the interviewer's name, it is categorized as an
        interviewer audio file.
    - If the file name contains "audio_only", it is categorized as a combined audio file.

    Defaults to uncategorized if the file name does not match any of the above criteria.
    """
    files: Dict[str, List[Path]] = {}

    files["combined"] = []
    files["participant"] = []
    files["interviewer"] = []

    uncategorized_files: List[Path] = []

    for file in audio_files:
        base_name = file.name
        base_name = base_name.split(".")[0]

        if base_name == "audio_only":
            files["combined"].append(file)
            continue

        if subject_id in file.name:
            files["participant"].append(file)
            continue

        if "interviewer" in file.name.lower():
            files["interviewer"].append(file)
            continue

        uncategorized_files.append(file)

    unknown_files: List[Path] = []
    for uncategorized_file in uncategorized_files:
        unassigned = True
        base_name = uncategorized_file.name
        base_name = base_name.split(".")[0]
        last_part = base_name.split("_")[-1]

        if last_part.isdigit() and unassigned:
            unassigned = False
            files["combined"].append(uncategorized_file)

        if len(base_name.split("_")) == 1 and unassigned:
            unassigned = False
            pattern = r"audio\d+"

            if re.match(pattern, base_name):
                unassigned = False
                files["combined"].append(uncategorized_file)

        if unassigned:
            unknown_files.append(uncategorized_file)

    files["uncategorized"] = []
    if len(unknown_files) == 1:
        if len(files["participant"]) == 0:
            files["participant"] = unknown_files
        elif len(files["interviewer"]) == 0:
            files["interviewer"] = unknown_files
        elif len(files["combined"]) == 0:
            files["combined"] = unknown_files
        else:
            files["uncategorized"] = unknown_files
    else:
        files["uncategorized"] = unknown_files

    return files


def fetch_interview_files(interview: Interview) -> List[InterviewFile]:
    """
    Fetches the interview files for a given interview.

    Args:
        interview (Interview): The interview object.

    Returns:
        List[InterviewFile]: A list of InterviewFile objects.
    """

    interview_files: List[InterviewFile] = []
    subject_id = interview.subject_id

    interview_path = interview.interview_path
    # list all files in the directory
    files = [f for f in interview_path.iterdir() if f.is_file()]

    audio_files: List[Path] = []
    video_files: List[Path] = []
    for file in files:
        base_name = file.name
        base_name_parts = base_name.split(".")
        if base_name_parts[-1] == "mp4":
            video_files.append(file)
        elif base_name_parts[-1] == "m4a":
            audio_files.append(file)

    categorized_audio_files = catogorize_audio_files(
        audio_files=audio_files, subject_id=subject_id
    )

    # Add the audio files
    for tag, audio_files in categorized_audio_files.items():
        for audio_file in audio_files:
            interview_file = InterviewFile(
                interview_path=interview_path,
                interview_file=audio_file,
                tags=f"audio,{tag}",
            )
            interview_files.append(interview_file)

    for video_file in video_files:
        interview_file = InterviewFile(
            interview_path=interview_path, interview_file=video_file, tags="video"
        )
        interview_files.append(interview_file)

    return interview_files


def fetch_interviews(
    config_file: Path, subject_id: str, study_id: str
) -> List[Interview]:
    """
    Fetches the interviews for a given subject ID.

    Args:
        config_file (Path): The path to the config file.
        subject_id (str): The subject ID.

    Returns:
        List[Interview]: A list of Interview objects.
    """
    config_params = config(path=config_file, section="general")
    data_root = Path(config_params["data_root"])

    study_path: Path = data_root / "PROTECTED" / study_id
    interview_types: List[InterviewType] = [InterviewType.OPEN, InterviewType.PSYCHS]

    interviews: List[Interview] = []
    for interview_type in interview_types:
        interview_type_path = (
            study_path / "raw" / subject_id / "interviews" / interview_type.value
        )

        if not interview_type_path.exists():
            logger.warning(
                f"{subject_id}: Could not find {interview_type.value} interviews: \
{interview_type_path} does not exist."
            )
            continue
        interview_dirs = [d for d in interview_type_path.iterdir() if d.is_dir()]

        for interview_dir in interview_dirs:
            base_name = interview_dir.name
            parts = base_name.split(" ")

            try:
                date_dt = date.fromisoformat(parts[0])
                # Time is of the form HH.MM.SS
                time_dt = time.fromisoformat(parts[1].replace(".", ":"))
                interview_datetime = datetime.combine(date_dt, time_dt)
            except ValueError:
                logger.warning(
                    f"{subject_id}: Could not parse date and time from {base_name}. Skipping..."
                )
                continue
            consent_date_s = core.get_consent_date_from_subject_id(
                config_file=config_file, subject_id=subject_id, study_id=study_id
            )
            if consent_date_s is None:
                logger.warning(f"Could not find consent date for {subject_id}")
                continue
            consent_date = datetime.strptime(consent_date_s, "%Y-%m-%d")

            interview_name = dpdash.get_dpdash_name(
                study=study_id,
                subject=subject_id,
                data_type=f"{interview_type.value}Interview",
                consent_date=consent_date,
                event_date=interview_datetime,
            )

            interview = Interview(
                interview_name=interview_name,
                interview_path=interview_dir,
                interview_datetime=interview_datetime,
                interview_type=interview_type,
                subject_id=subject_id,
                study_id=study_id,
            )

            interviews.append(interview)

    return interviews


def hash_file_worker(interview_file: InterviewFile) -> File:
    """
    Hashes the file and returns a File object.

    Args:
        interview_file (InterviewFile): The interview file to hash.
    """
    file = File(file_path=interview_file.interview_file)
    return file


def generate_queries(
    interviews: List[Interview],
    interview_files: List[InterviewFile],
    progress: Progress,
) -> List[str]:
    """
    Generates the SQL queries to insert the interview files into the database.

    Args:
        interviews (List[Interview]): A list of Interview objects.
        interview_files (List[InterviewFile]): A list of InterviewFile objects.
    """

    files: List[File] = []

    logger.info("Hashing files...")

    num_processes = multiprocessing.cpu_count() / 2
    logger.info(f"Using {num_processes} processes")
    with multiprocessing.Pool(processes=int(num_processes)) as pool:
        task = progress.add_task("Hashing files...", total=len(interview_files))
        for result in pool.imap_unordered(hash_file_worker, interview_files):
            files.append(result)
            progress.update(task, advance=1)
        progress.remove_task(task)

    sql_queries = []
    logger.info("Generating SQL queries...")
    # Insert the files
    for file in files:
        sql_queries.append(file.to_sql())

    # Insert the interviews
    for interview in interviews:
        sql_queries.append(interview.to_sql())

    # Insert the interview files
    for interview_file in interview_files:
        sql_queries.append(interview_file.to_sql())

    return sql_queries


def import_interviews(config_file: Path, study_id: str, progress: Progress) -> None:
    """
    Imports the interviews into the database.

    Args:
        config_file (Path): The path to the configuration file.
    """

    # Get the subjects
    subjects = core.get_subject_ids(config_file=config_file, study_id=study_id)

    # Get the interviews
    logger.info(f"Fetching interviews for {study_id}")
    interviews: List[Interview] = []

    task = progress.add_task("Fetching interviews for subjects", total=len(subjects))
    for subject_id in subjects:
        progress.update(
            task, advance=1, description=f"Fetching {subject_id}'s interviews..."
        )
        interviews.extend(
            fetch_interviews(
                config_file=config_file, subject_id=subject_id, study_id=study_id
            )
        )

    # Get the interview files
    logger.info("Fetching interview files...")
    interview_files: List[InterviewFile] = []

    # TODO: Remove this
    num_interviews = 2
    interview_counter = 0
    task = progress.add_task("Fetching interview files...", total=len(interviews))
    for interview in interviews:
        if interview_counter > num_interviews:
            break
        interview_counter += 1
        progress.update(task, advance=1)
        interview_files.extend(fetch_interview_files(interview=interview))

    # Generate the SQL queries to import the interview files
    sql_queries = generate_queries(
        interviews=interviews, interview_files=interview_files, progress=progress
    )

    # Execute the SQL queries
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

    study_ids = orchestrator.get_studies(config_file=config_file)
    with utils.get_progress_bar() as progress:
        study_task = progress.add_task("Importing interviews...", total=len(study_ids))
        for study_id in study_ids:
            progress.update(
                study_task,
                advance=1,
                description=f"Importing interviews for {study_id}...",
            )
            import_interviews(
                config_file=config_file, study_id=study_id, progress=progress
            )

    logger.info("[bold green]Done!", extra={"markup": True})
