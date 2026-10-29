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
    if parent.name == "dpinterview":
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
from typing import Dict, List, Tuple

from rich.logging import RichHandler
from rich.progress import Progress

from pipeline import core, orchestrator
from pipeline.helpers import cli, db, dpdash, utils
from pipeline.helpers.config import config
from pipeline.models.files import File
from pipeline.models.interview_files import InterviewFile
from pipeline.models.interview_parts import InterviewParts
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


def fetch_interview_files(
    config_file: Path, interview_part: InterviewParts
) -> List[InterviewFile]:
    """
    Fetches the interview files for a given interview.

    Args:
        interview (Interview): The interview object.

    Returns:
        List[InterviewFile]: A list of InterviewFile objects.
    """

    interview_files: List[InterviewFile] = []

    interview_path = interview_part.interview_path
    # list all files in the directory
    files = [f for f in interview_path.iterdir() if f.is_file()]

    av_files: List[Path] = []
    for file in files:
        base_name = file.name
        base_name_parts = base_name.split(".")
        if base_name_parts[-2] == "mkv":
            av_files.append(file)

    for av_file in av_files:
        interview_file = InterviewFile(
            interview_path=interview_path, interview_file=av_file, tags="video,audio"
        )
        interview_files.append(interview_file)

    return interview_files


def handle_multi_part_interviews(
    interview_parts: List[InterviewParts]
) -> List[InterviewParts]:
    """
    Sorts and groups the interviews by day, fixing part numbers chronologically.

    Args:
        interview_parts (List[InterviewParts]): A list of InterviewParts objects, with arbitrary part numbers.

    Returns:
        List[InterviewParts]: A list of InterviewParts objects, with part numbers fixed.
    """
    # sort the interviews by datetime
    interview_parts.sort(key=lambda x: x.interview_datetime)

    for idx, interview in enumerate(interview_parts):
        if idx == 0:
            prev_interview = None
            # interview_name_w_session = f"{interview.interview_name}-part{1:03d}"
            # interview.interview_name = interview_name_w_session
            interview.interview_part = 1
        else:
            prev_interview = interview_parts[idx - 1]

            current_interview_date = interview.interview_datetime.date()
            prev_interview_date = prev_interview.interview_datetime.date()

            if prev_interview_date == current_interview_date:
                prev_interview_session_number = prev_interview.interview_part
                interview.interview_part = prev_interview_session_number + 1
                # interview_name_w_session = f"{interview.interview_name}-part{interview.interview_part:03d}"
                # interview.interview_name = interview_name_w_session
            else:
                prev_interview = None
                interview.interview_part = 1
                # interview_name_w_session = f"{interview.interview_name}-part{1:03d}"
                # interview.interview_name = interview_name_w_session

    return interview_parts


def fetch_interviews(config_file: Path, subject_id: str) -> List[InterviewParts]:
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
    study_id = config_params["study"]

    study_path: Path = data_root / "PROTECTED" / study_id
    onsite_interview_path: Path = study_path / subject_id / "onsite_interview" / "raw"

    if not onsite_interview_path.exists():
        logger.warning(f"Could not find onsite interview path for {subject_id}")
        return []

    interview_parts: List[InterviewParts] = []
    interview_dirs = [d for d in onsite_interview_path.iterdir() if d.is_dir()]

    for interview_dir in interview_dirs:
        base_name = interview_dir.name  # CAMI343_YYMMDD_HH_MM_SS
        parts = base_name.split("_")

        try:
            date_dt = datetime.strptime(parts[1], "%y%m%d").date()
        except ValueError:
            try:
                date_dt = datetime.strptime(parts[1], "%Y%m%d").date()
            except ValueError:
                logger.error(f"Could not parse date for {base_name}")
                raise

        try:
            time_dt = datetime.strptime(f"{parts[2]}:{parts[3]}:{parts[4]}", "%H:%M:%S").time()
        except ValueError:
            logger.error(f"Could not parse time for {base_name}")
            raise
        interview_datetime = datetime.combine(date_dt, time_dt)

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
            data_type="onsiteInterview",
            category=None,
            consent_date=consent_date,
            event_date=interview_datetime,
        )

        # interview = Interview(
        #     interview_name=interview_name,
        #     interview_path=interview_dir,
        #     interview_datetime=interview_datetime,
        #     interview_type=InterviewType.ONSITE,
        #     subject_id=subject_id,
        #     study_id=study_id,
        # )

        # interviews.append(interview)

        # truncate time for day calculation (same as for ampscz logic)
        interview_datetime_for_day = interview_datetime.replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        
        interview_day = dpdash.get_days_between_dates(
                consent_date=consent_date, event_date=interview_datetime
            )

        interview_part = InterviewParts(
            interview_name=interview_name,
            interview_path=interview_dir,
            interview_day=interview_day,          # CAMI appears single-part; or parse if needed
            interview_part=1,
            interview_datetime=interview_datetime,
            is_primary=True,
            is_duplicate=False,
        )

        interview_parts.append(interview_part)

    interview_parts = handle_multi_part_interviews(interview_parts)

    return interview_parts


def hash_file_worker(interview_file: InterviewFile) -> File:
    """
    Hashes the file and returns a File object.

    Args:
        interview_file (InterviewFile): The interview file to hash.
    """
    file = File(file_path=interview_file.interview_file)
    return file


def generate_queries(interview_parts: List[InterviewParts], interview_files: List[InterviewFile]):
    """
    Generates the SQL queries to insert the interview files into the database.

    Args:
        interview_parts (List[InterviewParts]): A list of InterviewParts objects.
        interview_files (List[InterviewFile]): A list of InterviewFile objects.
    """

    files: List[File] = []

    logger.info("Hashing files...")

    num_processes = multiprocessing.cpu_count() / 4
    logger.info(f"Using {num_processes} processes")
    with multiprocessing.Pool(processes=int(num_processes)) as pool:
        with utils.get_progress_bar() as progress:
            task = progress.add_task("Hashing files...", total=len(interview_files))
            for result in pool.imap_unordered(hash_file_worker, interview_files):
                files.append(result)
                progress.update(task, advance=1)

    sql_queries = []
    logger.info("Generating SQL queries...")
    # Insert the files
    for file in files:
        sql_queries.append(file.to_sql())

    # --- CHANGE START: interviews list was not defined; derive Interview rows from interview_parts (AMPSCZ pattern)
    interviews: List[Interview] = []
    seen_names = set()

    for part in interview_parts:
        name = part.interview_name
        
        if name in seen_names:
            continue
        seen_names.add(name)

        dp = dpdash.parse_dpdash_name(name)
        interview_type = InterviewType(utils.camel_case_split(dp["data_type"])[0])

        interviews.append(
            Interview(
                interview_name=name,
                # interview_path=part.interview_path,
                # interview_datetime=part.interview_datetime,
                interview_type=interview_type,
                subject_id=dp["subject"],
                study_id=dp["study"],
            )
        )
    # --- CHANGE END
    
    # Insert the interviews
    for interview in interviews:
        sql_queries.append(interview.to_sql())

    # --- CHANGE START: insert interview_parts before interview_files (FK dependency on interview_path)
    for part in interview_parts:
        sql_queries.append(part.to_sql())
    # --- CHANGE END

    # Insert the interview files
    for interview_file in interview_files:
        sql_queries.append(interview_file.to_sql())

    return sql_queries


def import_interviews(config_file: Path) -> None:
    """
    Imports the interviews into the database.

    Args:
        config_file (Path): The path to the configuration file.
    """
    config_params = config(path=config_file, section="general")
    study_id = config_params["study"]

    # Get the subjects
    subjects = core.get_subject_ids(config_file=config_file, study_id=study_id)

    # Get the interviews
    logger.info(f"Fetching interviews for {study_id}")
    
    # interviews: List[Interview] = []
    # all_files: list[File] = []
    all_interview_parts: list[InterviewParts] = []
    all_interview_files: list[InterviewFile] = []

    with utils.get_progress_bar() as progress:
        task = progress.add_task(
            "Fetching interviews for subjects", total=len(subjects)
        )
        
        for subject_id in subjects:
            progress.update(
                task, advance=1, description=f"Fetching {subject_id}'s interviews..."
            )
            
            # 1) Fetch interviews for the subject
            interview_parts = fetch_interviews(config_file=config_file, subject_id=subject_id)
            all_interview_parts.extend(interview_parts)
            
            # 2) Fetch interview files for each interview
            for interview_part in interview_parts:
                interview_files = fetch_interview_files(config_file=config_file, interview_part=interview_part)
                all_interview_files.extend(interview_files)

            # interviews.extend(
            #     fetch_interviews(config_file=config_file, subject_id=subject_id)
            # )

        # # Get the interview files
        # logger.info("Fetching interview files...")
        # interview_files: List[InterviewFile] = []

        # task = progress.add_task("Fetching interview files...", total=len(interview_parts))
        # for interview_part in interview_parts:
        #     progress.update(task, advance=1)
        #     interview_files.extend(
        #         fetch_interview_files(interview_part=interview_part, config_file=config_file)
        #     )

    # DEBUG: confirm computed values BEFORE SQL
    for p in all_interview_parts[:20]:
        logger.info(
            f"DEBUG InterviewParts: name={p.interview_name} "
            f"day={p.interview_day} part={p.interview_part} dt={p.interview_datetime}"
        )

    # DEBUG: show the SQL that will be executed for interview_parts
    logger.info("DEBUG: first 5 interview_parts SQL statements:")
    for p in all_interview_parts[:5]:
        logger.info(p.to_sql())
    
    # Generate the SQL queries to import the interview files
    sql_queries = generate_queries(
        interview_parts=all_interview_parts, interview_files=all_interview_files
    )

    # Execute the SQL queries
    db.execute_queries(
        config_file=config_file, queries=sql_queries, show_commands=False
    )


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

    import_interviews(config_file=config_file)

    logger.info("[bold green]Done!", extra={"markup": True})
