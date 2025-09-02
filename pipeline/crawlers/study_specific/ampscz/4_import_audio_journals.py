#!/usr/bin/env python
"""
Looks for audio journals under:
PHOENIX/PROTECTED/*/raw/*/phone/*_sound_*.mp3
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
import json
import logging
import multiprocessing
import re
from datetime import datetime
from typing import Dict, List, Tuple, Optional

import pytz
from rich.logging import RichHandler
from rich.progress import Progress

from pipeline import core, orchestrator
from pipeline.helpers import cli, db, dpdash, utils
from pipeline.models.audio_journals import AudioJournal
from pipeline.models.files import File
from pipeline.models.subjects import Subject

MODULE_NAME = "import_audio_journals"
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


study_timezones: Dict[str, str] = {
    "PronetBI": "America/New_York",
    "PronetCA": "Canada/Mountain",
    "PronetCM": "Europe/London",
    "PronetGA": "America/New_York",
    "PronetHA": "America/New_York",
    "PronetIR": "America/Los_Angeles",
    "PronetKC": "Europe/London",
    "PronetLA": "America/Los_Angeles",
    "PronetMA": "Europe/Madrid",
    "PronetMT": "Canada/Eastern",
    "PronetMU": "Europe/Berlin",
    "PronetNC": "America/New_York",
    "PronetNL": "America/New_York",
    "PronetNN": "America/Chicago",
    "PronetOR": "America/Los_Angeles",
    "PronetPA": "America/New_York",
    "PronetPI": "America/New_York",
    "PronetPV": "Europe/Rome",
    "PronetSD": "America/Los_Angeles",
    "PronetSF": "America/Los_Angeles",
    "PronetSH": "Asia/Shanghai",
    "PronetSI": "America/New_York",
    "PronetSL": "Asia/Seoul",
    "PronetTE": "America/New_York",
    "PronetUR": "America/New_York",
    "PronetWU": "US/Central",
    "PronetYA": "America/New_York",

    "PrescientBM": "Europe/London",
    "PrescientCG": "Europe/Berlin",
    "PrescientCP": "Europe/Berlin",
    "PrescientGW": "Asia/Seoul",
    "PrescientHK": "Asia/Hong_Kong",
    "PrescientJE": "Europe/Berlin",
    "PrescientLS": "Europe/Zurich",
    "PrescientME": "Australia/Melbourne",
    "PrescientSG": "Asia/Singapore",
    "PrescientST": "America/Santiago",
}


def get_journal_timestamp_from_mindlamp_json(
    audio_journal: Path,
    json_file_path: Path,
    timezone: str
) -> Optional[datetime]:
    """
    Get the timestamp from the Mindlamp JSON file.

    Args:
        audio_journal (Path): The path to the audio journal file.
        json_file_path (Path): The path to the JSON file.
        timezone (str): The timezone of the study.

    Returns:
        Optional[datetime]: The timestamp of the audio journal.
    """
    audio_journal_timestamp = None
    with open(json_file_path, "r", encoding="utf-8") as json_file:
        json_data = json.load(json_file)

        for event in json_data:
            if "static_data" in event and "url" in event["static_data"]:
                url: str = event["static_data"]["url"]
                if url.lower() in audio_journal.name.lower():
                    unix_timestamp = event["timestamp"]  # 1739593798406
                    audio_journal_timestamp = datetime.fromtimestamp(
                        unix_timestamp / 1000,
                        tz=pytz.timezone(timezone),
                    )
                    break

    return audio_journal_timestamp


def fetch_journals(
    config_file: Path, subject_id: str, study_id: str
) -> List[AudioJournal]:
    """
    Fetches the AudioJournals for a given subject ID.

    Args:
        config_file (Path): The path to the config file.
        subject_id (str): The subject ID.

    Returns:
        List[AudioJournal]: A list of AudioJournal objects.
    """
    config_params = utils.config(path=config_file, section="general")
    data_root = Path(config_params["data_root"])

    study_path: Path = data_root / "PROTECTED" / study_id
    audio_journal_root_path = study_path / "raw" / subject_id / "phone"

    if not audio_journal_root_path.exists():
        logger.debug(
            f"No audio journal path for {subject_id} at {audio_journal_root_path}"
        )
        return []

    audio_journal_path = list(audio_journal_root_path.glob("*_sound_*.mp3"))
    audio_journals: List[AudioJournal] = []

    study_timezone = study_timezones.get(study_id, None)
    if study_timezone is None:
        logger.error(f"No timezone found for {study_id}")
        sys.exit(1)

    subject_consent_date = Subject.get_consent_date(
        study_id=study_id, subject_id=subject_id, config_file=config_file
    )
    subject_consent_date = pytz.timezone(study_timezone).localize(
        subject_consent_date  # type: ignore
    )  # convert to timezone aware datetime
    if subject_consent_date is None:
        logger.error(f"No consent date found for {subject_id} - skipping...")
        return []

    if len(audio_journal_path) == 0:
        logger.debug(
            f"No audio journal found for {subject_id} at {audio_journal_root_path}"
        )
        return []
    else:
        logger.info(f"Found {len(audio_journal_path)} audio journals for {subject_id}")

    for audio_journal in audio_journal_path:
        audio_journal_basename = (
            audio_journal.name
        )  # U3121181823_PronetYA_activity_2025_01_07_sound_0.mp3

        # get relevant JSON file replacing *sound_*.mp3 *.json
        # U3121181823_PronetYA_activity_2025_01_07.json

        json_file_name = re.sub(r"_sound_\d+\.mp3$", ".json", audio_journal.name)
        json_file_path = audio_journal.parent / json_file_name
        if not json_file_path.exists():
            logger.error(
                f"No JSON file found for {audio_journal} at {json_file_path}. Skipping..."
            )
            continue

        audio_journal_timestamp = get_journal_timestamp_from_mindlamp_json(
            audio_journal=audio_journal,
            json_file_path=json_file_path,
            timezone=study_timezone,
        )

        audio_journal_parts = audio_journal_basename.split("_")
        if audio_journal_timestamp is None:
            audio_journal_timestamp = datetime.strptime(
                f"{audio_journal_parts[3]}-{audio_journal_parts[4]}-{audio_journal_parts[5]}",
                "%Y-%m-%d",
            )
            audio_journal_timestamp = pytz.timezone(study_timezone).localize(
                audio_journal_timestamp
            )
            logger.warning(
                f"No timestamp found for {audio_journal} at "
                f"{json_file_path}. Using date from filename: {audio_journal_timestamp}"
            )

        journal_day = (audio_journal_timestamp - subject_consent_date).days + 1

        journal_session = audio_journal_parts[-1].split(".")[0]
        journal_session = int(journal_session) + 1

        audio_journal_name = dpdash.get_dpdash_name(
            study=study_id,
            subject=subject_id,
            data_type="audioJournal",
            consent_date=subject_consent_date,
            time_range=f"day{journal_day:04d}",
        )
        # audio_journal_name = f"{audio_journal_name}-session{journal_session:04d}"
        audio_journal = AudioJournal(
            aj_path=audio_journal,
            aj_name=audio_journal_name,
            aj_datetime=audio_journal_timestamp,
            aj_day=journal_day,
            aj_session=journal_session,
            subject_id=subject_id,
            study_id=study_id,
        )
        audio_journals.append(audio_journal)

    audio_journals.sort(key=lambda journal: journal.aj_datetime)
    unique_days = sorted(set(journal.aj_day for journal in audio_journals))

    # Mark session numbers in ascending order, restarting from 1 for each day
    # Fizes session numbers after timezone adjustment
    sorted_journals: List[AudioJournal] = []
    for day in unique_days:
        day_journals = [journal for journal in audio_journals if journal.aj_day == day]

        # Check if no time components, if time is 00:00:00, skip reset
        if any(
            journal.aj_datetime.time() == datetime.min.time()
            for journal in day_journals
        ):
            logger.warning(
                f"Skipping session number reset for {subject_id} on day {day} - No time stamp"
            )
            continue

        session_number = 1
        for journal in day_journals:
            journal.aj_session = session_number
            session_number += 1
            journal.aj_name = f"{journal.aj_name}-session{journal.aj_session:04d}"
            sorted_journals.append(journal)

    return sorted_journals


def hash_file_worker(params: Tuple[AudioJournal, Path]) -> File:
    """
    Hashes the file and returns a File object.

    Args:
        params (Tuple[AudioJournal, Path]): A tuple containing the AudioJournal
            and the path to the config file.
    """
    journal_file, config_file = params
    with_hash = orchestrator.is_crawler_hashing_required(config_file=config_file)

    file = File(file_path=journal_file.aj_path, with_hash=with_hash)
    return file


def generate_queries(
    journals: List[AudioJournal],
    config_file: Path,
    progress: Progress,
) -> List[str]:
    """
    Generates the SQL queries to insert the interview files into the database.

    Args:
        journals (List[AudioJournal]): The list of AudioJournal objects.
        config_file (Path): The path to the configuration file.
        progress (Progress): The progress bar.
    """

    files: List[File] = []

    if orchestrator.is_crawler_hashing_required(config_file=config_file):
        logger.info("Hashing files...")
    else:
        logger.info("Skipping hashing files...")

    params = [(journal, config_file) for journal in journals]
    logger.info(f"Have {len(params)} files to hash")

    num_processes = multiprocessing.cpu_count() / 2
    logger.info(f"Using {num_processes} processes")
    with multiprocessing.Pool(processes=int(num_processes)) as pool:
        task = progress.add_task("Hashing files...", total=len(params))
        for result in pool.imap_unordered(hash_file_worker, params):
            files.append(result)
            progress.update(task, advance=1)
        progress.remove_task(task)

    sql_queries = []
    logger.info("Generating SQL queries...")
    # Insert the files
    for file in files:
        sql_queries.append(file.to_sql())

    # Insert the journals
    for journal in journals:
        sql_queries.append(journal.to_sql())

    return sql_queries


def import_journals(config_file: Path, study_id: str, progress: Progress) -> None:
    """
    Imports the Audio Journals into the database.

    Args:
        config_file (Path): The path to the configuration file.
    """

    # Get the subjects
    subjects = core.get_subject_ids(config_file=config_file, study_id=study_id)

    # Get the journals
    logger.info(f"Fetching journals for {study_id}")
    journals: List[AudioJournal] = []

    task = progress.add_task("Fetching journals for subjects", total=len(subjects))
    for subject_id in subjects:
        progress.update(
            task, advance=1, description=f"Fetching {subject_id}'s journals..."
        )
        journals.extend(
            fetch_journals(
                config_file=config_file, subject_id=subject_id, study_id=study_id
            )
        )
    progress.remove_task(task)

    logger.info(f"Found {len(journals)} journals for {study_id}")

    # Generate the SQL queries
    sql_queries = generate_queries(
        journals=journals, config_file=config_file, progress=progress
    )

    # Execute the queries
    db.execute_queries(
        queries=sql_queries,
        config_file=config_file,
        silent=True,
        on_failure=lambda: (logger.error("Error executing queries")),
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog=MODULE_NAME, description="Import audio journals into the database."
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
        study_task = progress.add_task("Importing journals...", total=len(study_ids))
        for study_id in study_ids:
            progress.update(
                study_task,
                advance=1,
                description=f"Importing journals for {study_id}...",
            )
            import_journals(
                config_file=config_file, study_id=study_id, progress=progress
            )

    logger.info("Done!")
