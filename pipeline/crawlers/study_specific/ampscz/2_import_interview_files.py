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
from collections import Counter
from datetime import date, datetime, time
from typing import Dict, List, NamedTuple, Optional, Set, Tuple, Union

from rich.logging import RichHandler
from rich.progress import Progress

from pipeline import core, orchestrator
from pipeline.helpers import cli, db, dpdash, ffprobe, utils
from pipeline.helpers.config import config
from pipeline.models import datetime_overrides
from pipeline.models.files import File
from pipeline.models.interview_files import InterviewFile
from pipeline.models.interview_parts import InterviewParts
from pipeline.models.interviews import Interview, InterviewType

MODULE_NAME = "import_interview_files"
INSTANCE_NAME = MODULE_NAME

# Media that can be an interview's recording, loose in a type folder or in a folder
# (.wav is the timestamp-named recorder format, crawled separately)
MEDIA_SUFFIXES = (".mp4", ".m4a")

# Days a renamed folder's name date may sit from a runsheet entry and still be trusted
RUNSHEET_MATCH_WINDOW_DAYS = 1
# An entry with a part this close is already covered, as dpinterview-web's Missing dashboard counts it
RUNSHEET_CLAIM_WINDOW_DAYS = 10


class UndatedPath(NamedTuple):
    """A raw path with no timestamp in its name and no override, awaiting a metadata date."""

    path: Path
    error_code: str
    reason: str


class MetadataCandidate(NamedTuple):
    """An undated path with the part its metadata date would give it."""

    undated: UndatedPath
    part: InterviewParts
    is_video: bool


class Rejection(NamedTuple):
    """An undated path the metadata tier could not match, with the failure to record."""

    undated: UndatedPath
    error_code: str
    detail: str

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

        if (
            last_part.isdigit()
            and unassigned
            and "Audio Record" not in str(uncategorized_file)
        ):
            unassigned = False
            files["combined"].append(uncategorized_file)

        if len(base_name.split("_")) == 1 and unassigned:
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


def list_media_files(directory: Path) -> List[Path]:
    """
    Lists the recordings directly in a directory and in its 'Audio Record' subdirectory.

    Args:
        directory (Path): The directory to list.

    Returns:
        List[Path]: Non-hidden files with a MEDIA_SUFFIXES extension, in any case.
    """
    candidates = [f for f in directory.iterdir() if f.is_file()]
    audio_record_dir = directory / "Audio Record"
    if audio_record_dir.is_dir():
        candidates.extend(f for f in audio_record_dir.iterdir() if f.is_file())

    return [
        f for f in candidates
        if not f.name.startswith(".") and f.suffix.lower() in MEDIA_SUFFIXES
    ]


def find_media_dir(interview_path: Path) -> Union[Path, str]:
    """
    Finds the directory holding an interview folder's recordings: the folder
    itself, or its only subfolder with recordings (e.g. a nested Zoom folder).

    Args:
        interview_path (Path): The raw interview directory.

    Returns:
        Union[Path, str]: The directory, or why none could be chosen.
    """
    if list_media_files(interview_path):
        return interview_path

    media_subdirs = sorted(
        d for d in interview_path.iterdir()
        if d.is_dir()
        and d.name != "Audio Record"
        and not d.name.startswith(".")
        and list_media_files(d)
    )
    if len(media_subdirs) == 1:
        return media_subdirs[0]
    if not media_subdirs:
        return "No .mp4/.m4a files in the folder or any subfolder"
    names = ", ".join(d.name for d in media_subdirs)
    return f"Recordings in {len(media_subdirs)} subfolders, cannot choose one: {names}"


def is_combined_audio(audio_file: Path) -> bool:
    """
    Checks whether an audio file is Zoom's combined recording rather than one speaker's track.

    Args:
        audio_file (Path): The audio file.

    Returns:
        bool: True for 'audio_only' or 'audio<digits>' outside an 'Audio Record' folder.
    """
    if "Audio Record" in audio_file.parts:
        return False
    stem = audio_file.stem
    return stem == "audio_only" or re.fullmatch(r"audio\d+", stem) is not None


def choose_session_recordings(raw_path: Path) -> Union[List[Path], str]:
    """
    Picks the recordings that stand for a raw path's session: its videos, else its combined audio.

    Args:
        raw_path (Path): A loose media file or an undated interview folder.

    Returns:
        Union[List[Path], str]: The recordings, or why none can stand for a single session.
    """
    if raw_path.is_file():
        media = [raw_path]
    else:
        # find_media_dir ignores subfolders once the top level has media; a dated one may be a second session
        nested = sorted(
            d.name for d in raw_path.iterdir()
            if d.is_dir()
            and d.name != "Audio Record"
            and not d.name.startswith(".")
            and any(
                f.suffix.lower() in MEDIA_SUFFIXES and not f.name.startswith(".")
                for f in d.rglob("*")
            )
        )
        if nested and list_media_files(raw_path):
            return f"Recordings both at the top level and in subfolder(s): {', '.join(nested)}"
        media_dir = find_media_dir(raw_path)
        if not isinstance(media_dir, Path):
            return media_dir
        media = list_media_files(media_dir)

    videos = [f for f in media if f.suffix.lower() == ".mp4"]
    if videos:
        return videos
    combined = [f for f in media if f.suffix.lower() == ".m4a" and is_combined_audio(f)]
    if combined:
        return combined
    return "No video or combined-audio recording to date it by"


def date_by_recordings(config_file: Path, raw_path: Path) -> Union[Tuple[date, bool], str]:
    """
    Dates a raw path by its session recordings' creation_time, which must all share one day.

    Args:
        config_file (Path): The path to the configuration file.
        raw_path (Path): A loose media file or an undated interview folder.

    Returns:
        Union[Tuple[date, bool], str]: The UTC day and whether it is video, or why it cannot be dated.
    """
    recordings = choose_session_recordings(raw_path)
    if isinstance(recordings, str):
        return recordings

    days = set()
    for recording in recordings:
        day = ffprobe.get_creation_date(config_file=config_file, file_path=recording)
        if day is None:
            return f"No metadata creation date on {recording.name}"
        days.add(day)
    if len(days) > 1:
        return f"Recordings span several days: {', '.join(str(d) for d in sorted(days))}"
    return days.pop(), recordings[0].suffix.lower() == ".mp4"


def fetch_interview_files(
    interview_part: InterviewParts, config_file: Path, study_id: str
) -> List[InterviewFile]:
    """
    Fetches the interview files for a given interview.

    Args:
        interview (Interview): The interview object.
        config_file (Path): The path to the configuration file.
        study_id (str): The study ID, for ledger context if parsing fails.

    Returns:
        List[InterviewFile]: A list of InterviewFile objects.
    """

    interview_files: List[InterviewFile] = []

    dp_dash_dict = dpdash.parse_dpdash_name(
        interview_part.interview_name
    )
    subject_id = dp_dash_dict["subject"]
    if not isinstance(subject_id, str):
        error = ValueError(
            f"Could not parse subject ID from {interview_part.interview_name}"
        )
        logger.error(f"Could not parse subject ID from {interview_part.interview_name}")
        logger.error(f"dp_dash_dict: {dp_dash_dict}")
        logger.error(f"subject_id: {subject_id} - {type(subject_id)}")
        db.record_failure(
            config_file=config_file,
            stage=MODULE_NAME,
            error_code="subject_id_parse",
            identifier=interview_part.interview_name,
            error=error,
            identifier_type="interview_name",
            study_id=study_id,
        )
        raise error
    interview_path = interview_part.interview_path

    if interview_path.is_file():
        # A single-file part is the interview's combined recording; an mp4 also carries its video
        tags = "audio,combined"
        if interview_path.suffix.lower() == ".mp4":
            tags += ",video"
        interview_files.append(
            InterviewFile(
                interview_path=interview_path,
                interview_file=interview_path,
                tags=tags,
            )
        )
        return interview_files

    media_dir = find_media_dir(interview_path)
    if not isinstance(media_dir, Path):
        logger.warning(f"{subject_id}: {media_dir} ({interview_path.name})")
        db.record_failure(
            config_file=config_file,
            stage=MODULE_NAME,
            error_code="no_media_files",
            identifier=str(interview_path),
            error=media_dir,
            identifier_type="file_path",
            study_id=study_id,
            subject_id=subject_id,
        )
        return interview_files

    files = list_media_files(media_dir)
    audio_files = [f for f in files if f.suffix.lower() == ".m4a"]
    video_files = [f for f in files if f.suffix.lower() == ".mp4"]

    categorized_audio_files = catogorize_audio_files(
        audio_files=audio_files, subject_id=subject_id
    )

    # Add the audio files
    for tag, audio_files in categorized_audio_files.items():
        for audio_file in audio_files:
            tags = f"audio,{tag}"
            if "Audio Record" in audio_file.parts:
                tags += ",diarized"
            interview_file = InterviewFile(
                interview_path=interview_path,
                interview_file=audio_file,
                tags=tags,
            )
            interview_files.append(interview_file)

    for video_file in video_files:
        interview_file = InterviewFile(
            interview_path=interview_path, interview_file=video_file, tags="video"
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


def parse_name_datetime(name: str) -> Tuple[Optional[date], Optional[time]]:
    """
    Parses the 'YYYY-MM-DD HH.MM.SS' prefix of an interview folder name.

    Args:
        name (str): The folder name.

    Returns:
        Tuple[Optional[date], Optional[time]]: The date, and the time if it also parsed.
    """
    parts = name.split(" ")
    try:
        name_date = date.fromisoformat(parts[0])
    except ValueError:
        return None, None

    try:
        # time format: 14.35.34
        name_time = time.fromisoformat(parts[1].replace(".", ":"))
    except (ValueError, IndexError):
        name_time = None
    return name_date, name_time


def matches_runsheet(interview_day: int, expected_days: List[int]) -> bool:
    """
    Checks whether an interview day falls within the match window of any runsheet entry.

    Args:
        interview_day (int): The interview's day, counted from consent.
        expected_days (List[int]): The runsheet entries' days, counted from consent.

    Returns:
        bool: True if some entry is within RUNSHEET_MATCH_WINDOW_DAYS.
    """
    return any(
        abs(interview_day - expected_day) <= RUNSHEET_MATCH_WINDOW_DAYS
        for expected_day in expected_days
    )


def fetch_expected_days(
    config_file: Path, study_id: str, subject_id: str
) -> Dict[str, List[int]]:
    """
    Fetches a subject's runsheet entry days, by interview type.

    Args:
        config_file (Path): The path to the configuration file.
        study_id (str): The study ID.
        subject_id (str): The subject ID.

    Returns:
        Dict[str, List[int]]: Expected interview days keyed by InterviewType value.
    """
    query = f"""
        SELECT expected_interview_type, expected_interview_day
        FROM public.expected_interviews
        WHERE study_id = '{db.santize_string(study_id)}'
            AND subject_id = '{db.santize_string(subject_id)}';
    """
    df = db.execute_sql(config_file=config_file, query=query)

    expected_days: Dict[str, List[int]] = {}
    for row in df.itertuples(index=False):
        expected_days.setdefault(row.expected_interview_type, []).append(
            int(row.expected_interview_day)
        )
    return expected_days


def override_or_record_failure(
    config_file: Path,
    raw_path: Path,
    error_code: str,
    error: Union[str, Exception],
    study_id: str,
    subject_id: str,
) -> Optional[datetime]:
    """
    Returns the staff-confirmed datetime override for a raw path the crawler
    could not date itself, or records a pipeline failure and returns None.

    Args:
        config_file (Path): The path to the configuration file.
        raw_path (Path): The raw interview file or directory.
        error_code (str): The pipeline_failures error code to record if there is no override.
        error (Union[str, Exception]): The error to record if there is no override.
        study_id (str): The study ID.
        subject_id (str): The subject ID.

    Returns:
        Optional[datetime]: The override datetime, or None if none has been recorded.
    """
    override_datetime = lookup_override(
        config_file=config_file, raw_path=raw_path, subject_id=subject_id
    )
    if override_datetime is None:
        record_undated(
            config_file=config_file,
            raw_path=raw_path,
            error_code=error_code,
            error=error,
            study_id=study_id,
            subject_id=subject_id,
        )
    return override_datetime


def lookup_override(
    config_file: Path, raw_path: Path, subject_id: str
) -> Optional[datetime]:
    """
    Returns the staff-confirmed datetime override recorded for a raw path, if any.

    Args:
        config_file (Path): The path to the configuration file.
        raw_path (Path): The raw interview file or directory.
        subject_id (str): The subject ID, for logging.

    Returns:
        Optional[datetime]: The override datetime, or None if none has been recorded.
    """
    override_datetime = datetime_overrides.get_override_datetime(
        config_file=config_file, identifier=str(raw_path)
    )
    if override_datetime is not None:
        logger.info(
            f"{subject_id}: Using staff-confirmed datetime override for {raw_path.name}"
        )
    return override_datetime


def record_undated(
    config_file: Path,
    raw_path: Path,
    error_code: str,
    error: Union[str, Exception],
    study_id: str,
    subject_id: str,
) -> None:
    """
    Records a pipeline failure for a raw path the crawler could not date.

    Args:
        config_file (Path): The path to the configuration file.
        raw_path (Path): The raw interview file or directory.
        error_code (str): The pipeline_failures error code.
        error (Union[str, Exception]): The error to record.
        study_id (str): The study ID.
        subject_id (str): The subject ID.
    """
    logger.error(
        f"{subject_id}: Could not date {raw_path.name} ({error_code}). Skipping..."
    )
    db.record_failure(
        config_file=config_file,
        stage=MODULE_NAME,
        error_code=error_code,
        identifier=str(raw_path),
        error=error,
        identifier_type="file_path",
        study_id=study_id,
        subject_id=subject_id,
    )


def build_interview_part(
    interview_path: Path,
    actual_interview_datetime: datetime,
    interview_type: InterviewType,
    study_id: str,
    subject_id: str,
    consent_date: datetime,
) -> InterviewParts:
    """
    Builds the InterviewParts for a raw interview path recorded at a given datetime.

    Args:
        interview_path (Path): The raw interview file or directory.
        actual_interview_datetime (datetime): When the interview was recorded.
        interview_type (InterviewType): The interview type.
        study_id (str): The study ID.
        subject_id (str): The subject ID.
        consent_date (datetime): The subject's consent date.

    Returns:
        InterviewParts: The part, numbered 1 until handle_multi_part_interviews renumbers it.
    """
    # Ignore time information, to get accurate day
    interview_datetime = actual_interview_datetime.replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    interview_name = dpdash.get_dpdash_name(
        study=study_id,
        subject=subject_id,
        data_type=f"{interview_type.value}Interview",
        consent_date=consent_date,
        event_date=interview_datetime,
    )
    interview_day = dpdash.get_days_between_dates(
        consent_date=consent_date, event_date=interview_datetime
    )

    return InterviewParts(
        interview_name=interview_name,
        interview_path=interview_path,
        interview_day=interview_day,
        interview_part=1,
        interview_datetime=actual_interview_datetime,
    )


def match_name_dated_dirs(
    config_file: Path,
    name_dated_dirs: List[Tuple[Path, date]],
    dated_parts: List[InterviewParts],
    expected_days: List[int],
    interview_type: InterviewType,
    study_id: str,
    subject_id: str,
    consent_date: datetime,
) -> List[InterviewParts]:
    """
    Builds parts for renamed folders whose name date lands alone on a day that a runsheet
    entry or the folder's own recording metadata confirms.

    Args:
        config_file (Path): The path to the configuration file.
        name_dated_dirs (List[Tuple[Path, date]]): Folders with a name date but no time or override.
        dated_parts (List[InterviewParts]): This type's parts already dated by name, timestamp or override.
        expected_days (List[int]): This type's runsheet entry days, counted from consent.
        interview_type (InterviewType): The interview type.
        study_id (str): The study ID.
        subject_id (str): The subject ID.
        consent_date (datetime): The subject's consent date.

    Returns:
        List[InterviewParts]: The matched parts; the rest are recorded as day_taken or datetime_parse failures.
    """
    candidates = [
        build_interview_part(
            interview_path=interview_dir,
            actual_interview_datetime=datetime.combine(name_date, time()),
            interview_type=interview_type,
            study_id=study_id,
            subject_id=subject_id,
            consent_date=consent_date,
        )
        for interview_dir, name_date in name_dated_dirs
    ]
    parts_per_day = Counter(p.interview_day for p in dated_parts + candidates)

    matched: List[InterviewParts] = []
    for part in candidates:
        name_date = part.interview_datetime.date()
        # A shared day is usually the leftover original of a folder staff already renamed
        if parts_per_day[part.interview_day] > 1:
            error_code = "day_taken"
            error = f"Name date {name_date} shares its day with another {interview_type.value} part"
        elif not matches_runsheet(part.interview_day, expected_days):
            # Off the runsheet (late or rescheduled), the recordings can still vouch for the name
            dated = date_by_recordings(config_file=config_file, raw_path=part.interview_path)
            if (
                not isinstance(dated, str)
                and abs((dated[0] - name_date).days) <= RUNSHEET_MATCH_WINDOW_DAYS
            ):
                logger.info(
                    f"{subject_id}: Dated {part.interview_path.name} by its name, confirmed by its metadata"
                )
                matched.append(part)
                continue
            metadata = dated if isinstance(dated, str) else f"metadata date {dated[0]}"
            error_code = "datetime_parse"
            error = (
                f"Name date {name_date} has no {interview_type.value} runsheet entry "
                f"within {RUNSHEET_MATCH_WINDOW_DAYS} day(s), and is not confirmed: {metadata}"
            )
        else:
            logger.info(
                f"{subject_id}: Dated {part.interview_path.name} by its name, matched to the runsheet"
            )
            matched.append(part)
            continue

        record_undated(
            config_file=config_file,
            raw_path=part.interview_path,
            error_code=error_code,
            error=error,
            study_id=study_id,
            subject_id=subject_id,
        )
    return matched


def date_undated_paths(
    config_file: Path,
    undated_paths: List[UndatedPath],
    interview_type: InterviewType,
    study_id: str,
    subject_id: str,
    consent_date: datetime,
) -> Tuple[List[MetadataCandidate], List[Rejection]]:
    """
    Dates each undated path by its recordings' metadata.

    Args:
        config_file (Path): The path to the configuration file.
        undated_paths (List[UndatedPath]): Undated folders and loose files.
        interview_type (InterviewType): The interview type.
        study_id (str): The study ID.
        subject_id (str): The subject ID.
        consent_date (datetime): The subject's consent date.

    Returns:
        Tuple[List[MetadataCandidate], List[Rejection]]: The dated paths, and those that could not be dated.
    """
    candidates: List[MetadataCandidate] = []
    rejections: List[Rejection] = []
    for undated in undated_paths:
        dated = date_by_recordings(config_file=config_file, raw_path=undated.path)
        if isinstance(dated, str):
            rejections.append(Rejection(undated, undated.error_code, dated))
            continue
        metadata_date, is_video = dated
        part = build_interview_part(
            interview_path=undated.path,
            actual_interview_datetime=datetime.combine(metadata_date, time()),
            interview_type=interview_type,
            study_id=study_id,
            subject_id=subject_id,
            consent_date=consent_date,
        )
        candidates.append(MetadataCandidate(undated, part, is_video))
    return candidates, rejections


def pick_session_primaries(
    candidates: List[MetadataCandidate], taken_days: Set[int]
) -> Tuple[List[MetadataCandidate], List[Rejection]]:
    """
    Keeps one recording per metadata day: the only video, else the only audio.

    Args:
        candidates (List[MetadataCandidate]): The metadata-dated paths.
        taken_days (Set[int]): Days already holding a part dated another way.

    Returns:
        Tuple[List[MetadataCandidate], List[Rejection]]: The kept paths, and the rest.
    """
    by_day: Dict[int, List[MetadataCandidate]] = {}
    for candidate in candidates:
        by_day.setdefault(candidate.part.interview_day, []).append(candidate)

    kept: List[MetadataCandidate] = []
    rejections: List[Rejection] = []
    for interview_day, same_day in by_day.items():
        metadata_date = same_day[0].part.interview_datetime.date()
        videos = [c for c in same_day if c.is_video]
        if interview_day in taken_days:
            rejections.extend(
                Rejection(c.undated, "day_taken", f"Metadata date {metadata_date} shares its day with a dated part")
                for c in same_day
            )
        elif len(videos) == 1 or (not videos and len(same_day) == 1):
            primary = videos[0] if videos else same_day[0]
            kept.append(primary)
            # Same-day audio beside a video is that session's own audio
            rejections.extend(
                Rejection(c.undated, "day_taken", f"Metadata date {metadata_date} is {primary.undated.path.name}'s session")
                for c in same_day if c is not primary
            )
        else:
            rejections.extend(
                Rejection(c.undated, c.undated.error_code, f"{len(same_day)} recordings date to {metadata_date}; cannot choose one")
                for c in same_day
            )
    return kept, rejections


def match_to_entries(
    candidates: List[MetadataCandidate], unclaimed_days: List[int]
) -> Tuple[List[MetadataCandidate], List[Rejection]]:
    """
    Matches each path to the one unclaimed runsheet entry near its metadata date.

    Args:
        candidates (List[MetadataCandidate]): One path per metadata day.
        unclaimed_days (List[int]): Runsheet entry days no dated part covers.

    Returns:
        Tuple[List[MetadataCandidate], List[Rejection]]: Paths with an entry to themselves, and the rest.
    """
    nearby = [
        [d for d in unclaimed_days if abs(d - c.part.interview_day) <= RUNSHEET_MATCH_WINDOW_DAYS]
        for c in candidates
    ]
    claims = Counter(days[0] for days in nearby if len(days) == 1)

    matched: List[MetadataCandidate] = []
    rejections: List[Rejection] = []
    for candidate, days in zip(candidates, nearby):
        metadata_date = candidate.part.interview_datetime.date()
        code = candidate.undated.error_code
        if not days:
            rejections.append(Rejection(candidate.undated, code, (
                f"Metadata date {metadata_date} has no unmatched runsheet entry "
                f"within {RUNSHEET_MATCH_WINDOW_DAYS} day(s)"
            )))
        elif len(days) > 1:
            rejections.append(Rejection(candidate.undated, code, (
                f"Metadata date {metadata_date} is near {len(days)} runsheet entries"
            )))
        elif claims[days[0]] > 1:
            rejections.append(Rejection(candidate.undated, code, (
                f"Metadata date {metadata_date} shares its runsheet entry with another recording"
            )))
        else:
            matched.append(candidate)
    return matched, rejections


def match_by_metadata(
    config_file: Path,
    undated_paths: List[UndatedPath],
    dated_parts: List[InterviewParts],
    expected_days: List[int],
    interview_type: InterviewType,
    study_id: str,
    subject_id: str,
    consent_date: datetime,
) -> List[InterviewParts]:
    """
    Builds parts for undated folders and loose files whose recordings' metadata date lands
    alone on a runsheet entry no other part covers.

    Args:
        config_file (Path): The path to the configuration file.
        undated_paths (List[UndatedPath]): Undated folders and loose files of this type.
        dated_parts (List[InterviewParts]): This type's parts dated another way this pass.
        expected_days (List[int]): This type's runsheet entry days, counted from consent.
        interview_type (InterviewType): The interview type.
        study_id (str): The study ID.
        subject_id (str): The subject ID.
        consent_date (datetime): The subject's consent date.

    Returns:
        List[InterviewParts]: The matched parts; the rest are recorded as failures.
    """
    # Claims come from this pass only: a part matched on an earlier pass is undated again now
    unclaimed_days = [
        d for d in expected_days
        if not any(abs(d - p.interview_day) <= RUNSHEET_CLAIM_WINDOW_DAYS for p in dated_parts)
    ]

    matched: List[MetadataCandidate] = []
    if not unclaimed_days:
        # Nothing to fill, so skip probing entirely
        rejections = [
            Rejection(u, u.error_code, f"No unmatched {interview_type.value} runsheet entry to fill")
            for u in undated_paths
        ]
    else:
        started = datetime.now()
        candidates, rejections = date_undated_paths(
            config_file=config_file,
            undated_paths=undated_paths,
            interview_type=interview_type,
            study_id=study_id,
            subject_id=subject_id,
            consent_date=consent_date,
        )
        logger.info(
            f"{subject_id}: Read metadata for {len(undated_paths)} undated "
            f"{interview_type.value} path(s) in {(datetime.now() - started).total_seconds():.1f}s"
        )
        primaries, same_day_rejections = pick_session_primaries(
            candidates=candidates, taken_days={p.interview_day for p in dated_parts}
        )
        matched, entry_rejections = match_to_entries(
            candidates=primaries, unclaimed_days=unclaimed_days
        )
        rejections += same_day_rejections + entry_rejections

    for candidate in matched:
        logger.info(
            f"{subject_id}: Dated {candidate.undated.path.name} by its metadata, matched to the runsheet"
        )
    for rejection in rejections:
        record_undated(
            config_file=config_file,
            raw_path=rejection.undated.path,
            error_code=rejection.error_code,
            error=f"{rejection.undated.reason}; {rejection.detail}",
            study_id=study_id,
            subject_id=subject_id,
        )
    return [c.part for c in matched]


def fetch_interviews(
    config_file: Path, subject_id: str, study_id: str, is_active: Optional[bool]
) -> List[InterviewParts]:
    """
    Fetches the interviews for a given subject ID.

    Args:
        config_file (Path): The path to the config file.
        subject_id (str): The subject ID.
        is_active (Optional[bool]): Whether the subject is active, from
            core.get_subject_active_status() - fetched once per study by the
            caller rather than queried per subject here.

    Returns:
        List[Interview]: A list of Interview objects.
    """
    if is_active is not None and not is_active:
        logger.debug(f"{subject_id}: subject is not active, skipping interview fetch.")
        return []

    # Fetched once per subject rather than once per interview file - it's the
    # same value for every file below, and each call previously opened its own
    # fresh DB connection (db.get_db_connection() creates a new SQLAlchemy
    # engine per call), which was the dominant cost for subjects with many
    # interview files.
    consent_date_s = core.get_consent_date_from_subject_id(
        config_file=config_file, subject_id=subject_id, study_id=study_id
    )
    if consent_date_s is None:
        error = ValueError(f"Could not find consent date for {subject_id}")
        logger.error(str(error))
        db.record_failure(
            config_file=config_file,
            stage=MODULE_NAME,
            error_code="consent_date_missing",
            identifier=subject_id,
            error=error,
            identifier_type="subject",
            study_id=study_id,
        )
        return []
    consent_date = datetime.strptime(consent_date_s, "%Y-%m-%d")

    config_params = config(path=config_file, section="general")
    data_root = Path(config_params["data_root"])

    study_path: Path = data_root / "PROTECTED" / study_id
    interview_types: List[InterviewType] = [InterviewType.OPEN, InterviewType.PSYCHS]

    interview_parts: List[InterviewParts] = []
    # Fetched only once a renamed folder needs it, so most subjects make no extra query
    expected_days: Optional[Dict[str, List[int]]] = None
    for interview_type in interview_types:
        interview_type_path = (
            study_path / "raw" / subject_id / "interviews" / interview_type.value
        )

        if not interview_type_path.exists():
            # Routine: an active subject just hasn't had this interview type
            # yet. Not warning-worthy - the genuinely actionable version of
            # this ("runsheet says it happened, we don't have it") is already
            # surfaced by dpinterview-web's Missing issues dashboard, which
            # compares expected_interviews against what actually got imported.
            logger.debug(
                f"{subject_id}: Could not find {interview_type.value} interviews: \
{interview_type_path} does not exist."
            )
            continue
        type_dir_entries = list(interview_type_path.iterdir())
        interview_dirs = [d for d in type_dir_entries if d.is_dir()]

        interview_type_parts: List[InterviewParts] = []
        name_dated_dirs: List[Tuple[Path, date]] = []
        undated_paths: List[UndatedPath] = []
        for interview_dir in interview_dirs:
            name_date, name_time = parse_name_datetime(interview_dir.name)

            if name_date is not None and name_time is not None:
                actual_interview_datetime = datetime.combine(name_date, name_time)
            else:
                override_datetime = lookup_override(
                    config_file=config_file, raw_path=interview_dir, subject_id=subject_id
                )
                if override_datetime is not None:
                    actual_interview_datetime = override_datetime
                elif name_date is not None:
                    # A dropped time marks a hand-renamed folder: matched to the runsheet below
                    name_dated_dirs.append((interview_dir, name_date))
                    continue
                else:
                    undated_paths.append(UndatedPath(
                        interview_dir,
                        "datetime_parse",
                        f"No 'YYYY-MM-DD HH.MM.SS' prefix: {interview_dir.name}",
                    ))
                    continue

            interview_type_parts.append(
                build_interview_part(
                    interview_path=interview_dir,
                    actual_interview_datetime=actual_interview_datetime,
                    interview_type=interview_type,
                    study_id=study_id,
                    subject_id=subject_id,
                    consent_date=consent_date,
                )
            )

        wav_files = [
            f for f in type_dir_entries
            if f.is_file()
            and f.suffix.lower() == ".wav"
            and not f.name.startswith(".check_sum_")
        ]

        for wav_file in wav_files:
            interview_datetime_str = wav_file.stem  # YYYYMMDDHHMMSS
            try:
                actual_interview_datetime = datetime.strptime(
                    interview_datetime_str, "%Y%m%d%H%M%S"
                )
            except ValueError as e:
                override_datetime = override_or_record_failure(
                    config_file=config_file,
                    raw_path=wav_file,
                    error_code="datetime_parse",
                    error=e,
                    study_id=study_id,
                    subject_id=subject_id,
                )
                if override_datetime is None:
                    continue
                actual_interview_datetime = override_datetime

            interview_type_parts.append(
                build_interview_part(
                    interview_path=wav_file,
                    actual_interview_datetime=actual_interview_datetime,
                    interview_type=interview_type,
                    study_id=study_id,
                    subject_id=subject_id,
                    consent_date=consent_date,
                )
            )

        # Loose media is imported on a staff override, else dated by its metadata below
        loose_files = [
            f for f in type_dir_entries
            if f.is_file()
            and not f.name.startswith(".")
            and f.suffix.lower() in MEDIA_SUFFIXES
        ]
        for loose_file in loose_files:
            override_datetime = lookup_override(
                config_file=config_file, raw_path=loose_file, subject_id=subject_id
            )
            if override_datetime is None:
                undated_paths.append(UndatedPath(
                    loose_file,
                    "loose_file",
                    f"Not in a dated interview folder: {loose_file.name}",
                ))
                continue

            interview_type_parts.append(
                build_interview_part(
                    interview_path=loose_file,
                    actual_interview_datetime=override_datetime,
                    interview_type=interview_type,
                    study_id=study_id,
                    subject_id=subject_id,
                    consent_date=consent_date,
                )
            )

        # Runs after every other part of this type is dated, to see which days are taken
        if name_dated_dirs or undated_paths:
            if expected_days is None:
                expected_days = fetch_expected_days(
                    config_file=config_file, study_id=study_id, subject_id=subject_id
                )
            type_expected_days = expected_days.get(interview_type.value, [])
        if name_dated_dirs:
            interview_type_parts.extend(
                match_name_dated_dirs(
                    config_file=config_file,
                    name_dated_dirs=name_dated_dirs,
                    dated_parts=interview_type_parts,
                    expected_days=type_expected_days,
                    interview_type=interview_type,
                    study_id=study_id,
                    subject_id=subject_id,
                    consent_date=consent_date,
                )
            )
        # After name-dated folders, so their parts claim runsheet entries first
        if undated_paths:
            interview_type_parts.extend(
                match_by_metadata(
                    config_file=config_file,
                    undated_paths=undated_paths,
                    dated_parts=interview_type_parts,
                    expected_days=type_expected_days,
                    interview_type=interview_type,
                    study_id=study_id,
                    subject_id=subject_id,
                    consent_date=consent_date,
                )
            )

        interview_type_parts = handle_multi_part_interviews(interview_type_parts)
        interview_parts.extend(interview_type_parts)

    return interview_parts


def hash_file_worker(params: Tuple[InterviewFile, Path]) -> File:
    """
    Hashes the file and returns a File object.

    Args:
        params (Tuple[InterviewFile, Path]): A tuple containing the InterviewFile
            and the path to the config file.
    """
    interview_file, config_file = params
    with_hash = orchestrator.is_crawler_hashing_required(config_file=config_file)

    file = File(file_path=interview_file.interview_file, with_hash=with_hash)
    return file


def generate_queries(
    interview_parts: List[InterviewParts],
    interview_files: List[InterviewFile],
    config_file: Path,
    progress: Progress,
) -> List[str]:
    """
    Generates the SQL queries to insert the interview files into the database.

    Args:
        interviews (List[Interview]): A list of Interview objects.
        interview_files (List[InterviewFile]): A list of InterviewFile objects.
        config_file (Path): The path to the configuration file.
        progress (Progress): The progress bar.
    """

    files: List[File] = []

    if orchestrator.is_crawler_hashing_required(config_file=config_file):
        logger.info("Hashing files...")
    else:
        logger.info("Skipping hashing files...")

    params = [(interview_file, config_file) for interview_file in interview_files]

    num_processes = multiprocessing.cpu_count() / 2
    logger.info(f"Using {num_processes} processes")
    with multiprocessing.Pool(processes=int(num_processes)) as pool:
        task = progress.add_task("Hashing files...", total=len(interview_files))
        for result in pool.imap_unordered(hash_file_worker, params):
            files.append(result)
            progress.update(task, advance=1)
        progress.remove_task(task)

    sql_queries = []
    logger.info("Generating SQL queries...")
    # Insert the files
    for file in files:
        sql_queries.append(file.to_sql())

    # Create Interview Object from InterviewParts
    for interview_part in interview_parts:
        dp_dash_dict = dpdash.parse_dpdash_name(interview_part.interview_name)
        subject_id = dp_dash_dict["subject"]
        study_id = dp_dash_dict["study"]
        data_type = dp_dash_dict["data_type"]

        interview_type = InterviewType(utils.camel_case_split(data_type)[0])  # type: ignore
        interview = Interview(
            interview_name=interview_part.interview_name,
            interview_type=interview_type,
            subject_id=subject_id,  # type: ignore
            study_id=study_id,  # type: ignore
        )

        sql_queries.append(interview.to_sql())

    # Insert the interviews parts
    for interview_part in interview_parts:
        sql_queries.append(interview_part.to_sql())

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
    subject_active_status = core.get_subject_active_status(
        config_file=config_file, study_id=study_id
    )

    # Get the interviews
    logger.info(f"Fetching interviews for {study_id}")
    interview_parts: List[InterviewParts] = []

    task = progress.add_task("Fetching interviews for subjects", total=len(subjects))
    for subject_id in subjects:
        progress.update(
            task, advance=1, description=f"Fetching {subject_id}'s interviews..."
        )
        interview_parts.extend(
            fetch_interviews(
                config_file=config_file,
                subject_id=subject_id,
                study_id=study_id,
                is_active=subject_active_status.get(subject_id),
            )
        )
    progress.remove_task(task)

    # Get the interview files
    logger.info("Fetching interview files...")
    interview_files: List[InterviewFile] = []

    interview_counter = 0
    task = progress.add_task("Fetching interview files...", total=len(interview_parts))
    for interview_part in interview_parts:
        interview_counter += 1
        progress.update(task, advance=1)
        interview_files.extend(
            fetch_interview_files(
                interview_part=interview_part, config_file=config_file, study_id=study_id
            )
        )
    progress.remove_task(task)

    # Generate the SQL queries to import the interview files
    sql_queries = generate_queries(
        interview_parts=interview_parts,
        interview_files=interview_files,
        config_file=config_file,
        progress=progress,
    )

    # Execute the SQL queries
    db.execute_queries(
        config_file=config_file,
        queries=sql_queries,
        show_commands=False,
        failure_stage="import_interview_files:ampscz",
        failure_identifier=study_id,
        failure_identifier_type="study",
    )

    # Successfully-imported parts may have previously failed to date-parse
    # (recorded in pipeline_failures, then matched to a runsheet entry via a
    # staff-confirmed datetime_overrides row) - close the loop on both now
    # that the import actually succeeded. No-ops for normally-parsed files:
    # there's no matching pipeline_failures/datetime_overrides row to update.
    paths_with_files = {f.interview_path for f in interview_files}
    for interview_part in interview_parts:
        identifier = str(interview_part.interview_path)
        # A part without files keeps the no_media_files row just recorded for it
        if interview_part.interview_path in paths_with_files:
            db.resolve_failure(
                config_file=config_file, stage=MODULE_NAME, identifier=identifier
            )
        datetime_overrides.mark_consumed(config_file=config_file, identifier=identifier)


def mark_unique_interviews_as_primary(config_file: Path, study_id: str) -> None:
    """
    Each interview should have a unique name. If there are unique interviews,
    mark them as primary. Duplicate interviews will need to be manually
    resolved.

    Args:
        config_file (Path): The path to the configuration file.
        study_id (str): The study ID.

    Returns:
        None
    """
    query = f"""
    WITH duplicate_groups AS (
        SELECT ip.interview_name, ip.interview_day
        FROM public.interview_parts ip
        JOIN public.interviews i ON ip.interview_name = i.interview_name
        WHERE i.study_id = '{study_id}'
        GROUP BY ip.interview_name, ip.interview_day
        HAVING COUNT(*) > 1
    )
    UPDATE public.interview_parts ip
    SET is_primary = true
    FROM public.interviews i
    WHERE ip.interview_name = i.interview_name
    AND i.study_id = '{study_id}'
    AND (ip.interview_name, ip.interview_day) NOT IN (
        SELECT dg.interview_name, dg.interview_day
        FROM duplicate_groups dg
    );
    """

    db.execute_queries(config_file=config_file, queries=[query])


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

    # Without an [ffmpeg] binary path, ffprobe runs through singularity as in 02_metadata
    try:
        ffmpeg_bin_path = config(path=config_file, section="ffmpeg").get("ffmpeg_bin_path")
    except ValueError:
        ffmpeg_bin_path = None
    if ffmpeg_bin_path:
        ffprobe.set_ffmpeg_bin_path(ffmpeg_bin_path)
    if not ffprobe.can_probe(config_file):
        logger.warning("ffprobe is unavailable: undated folders and loose files will not be dated by metadata")

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
            logger.info(f"Marking unique interviews as primary for {study_id}")
            mark_unique_interviews_as_primary(
                config_file=config_file, study_id=study_id
            )

    logger.info("[bold green]Done!", extra={"markup": True})
