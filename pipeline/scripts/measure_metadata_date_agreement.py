#!/usr/bin/env python
"""
Measures how well file metadata dates agree with staff-confirmed datetime overrides.

Read-only. For every media file under each datetime_overrides identifier, compares the
day of ffprobe's creation_time and the file mtime against the override's day, writes
one row per file to a CSV, and prints agreement rates by file kind and date source.
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
from datetime import date, datetime, timezone
from typing import List, Optional, TypedDict

import pandas as pd
from rich.logging import RichHandler

from pipeline.helpers import cli, db, ffprobe, utils

MODULE_NAME = "measure_metadata_date_agreement"

# Every kind a datetime_overrides identifier can hold, including timestamp-named .wav
MEDIA_SUFFIXES = (".mp4", ".m4a", ".wav")
DATE_SOURCES = ("format_creation_day", "stream_creation_day", "mtime_day")

logger = logging.getLogger(MODULE_NAME)
logargs = {
    "level": logging.INFO,
    "format": "%(message)s",
    "handlers": [RichHandler(rich_tracebacks=True)],
}
logging.basicConfig(**logargs)

console = utils.get_console()


class ProbedDates(TypedDict):
    """Date-bearing fields ffprobe reports for one media file."""

    format_creation_time: Optional[str]
    stream_creation_time: Optional[str]
    date_tags: str
    duration_s: Optional[float]
    has_video: bool


def fetch_overrides(config_file: Path) -> pd.DataFrame:
    """
    Fetches every staff-confirmed override with the crawler error codes recorded for it.

    Args:
        config_file (Path): The path to the configuration file.

    Returns:
        pd.DataFrame: One row per override: identifier, study, subject, override_day, error_codes.
    """
    query = """
        SELECT
            dto.do_identifier AS identifier,
            dto.do_study_id AS study_id,
            dto.do_subject_id AS subject_id,
            dto.do_override_datetime::date AS override_day,
            (
                SELECT string_agg(DISTINCT pf.pf_error_code, ',')
                FROM pipeline_ledger.pipeline_failures pf
                WHERE pf.pf_identifier = dto.do_identifier
                    AND pf.pf_stage = 'import_interview_files'
            ) AS error_codes
        FROM pipeline_ledger.datetime_overrides dto
        ORDER BY dto.do_study_id, dto.do_subject_id, dto.do_identifier;
    """
    return db.execute_sql(config_file=config_file, query=query)


def list_media(identifier: Path) -> List[Path]:
    """
    Lists the media an override identifier covers: the file itself, or every media file under the folder.

    Args:
        identifier (Path): The raw file or directory the override was recorded for.

    Returns:
        List[Path]: Non-hidden files with a MEDIA_SUFFIXES extension, in any case.
    """
    if identifier.is_file():
        return [identifier]
    return sorted(
        f for f in identifier.rglob("*")
        if f.is_file()
        and not f.name.startswith(".")
        and f.suffix.lower() in MEDIA_SUFFIXES
    )


def probe_dates(config_file: Path, media_file: Path) -> Optional[ProbedDates]:
    """
    Runs ffprobe on a media file and pulls out its date-bearing fields.

    Args:
        config_file (Path): The path to the configuration file.
        media_file (Path): The file to probe.

    Returns:
        Optional[ProbedDates]: The fields, or None if ffprobe failed.
    """
    result = ffprobe.ffprobe(file_path=media_file, config_file=config_file)
    if result.return_code != 0 or not result.json.strip():
        logger.warning(f"ffprobe failed on {media_file}: {result.error.strip()}")
        return None

    metadata = json.loads(result.json)
    format_info = metadata.get("format", {})
    format_tags = format_info.get("tags", {})
    streams = metadata.get("streams", [])
    stream_creation_times = [
        s["tags"]["creation_time"] for s in streams
        if "creation_time" in s.get("tags", {})
    ]
    # Catch-all for vendor tags (e.g. com.apple.quicktime.creationdate carries a local offset)
    date_tags = {
        k: v for k, v in format_tags.items()
        if "date" in k.lower() or "time" in k.lower()
    }
    duration = format_info.get("duration")

    return ProbedDates(
        format_creation_time=format_tags.get("creation_time"),
        stream_creation_time=stream_creation_times[0] if stream_creation_times else None,
        date_tags=json.dumps(date_tags),
        duration_s=float(duration) if duration is not None else None,
        has_video=any(s.get("codec_type") == "video" for s in streams),
    )


def utc_day(timestamp: Optional[str]) -> Optional[date]:
    """
    Converts an ffprobe ISO-8601 timestamp to its UTC calendar day.

    Args:
        timestamp (Optional[str]): e.g. '2025-03-04T21:15:02.000000Z'.

    Returns:
        Optional[date]: The UTC day, or None if absent or unparseable.
    """
    if not timestamp:
        return None
    try:
        parsed = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).date()


def file_kind(media_file: Path) -> str:
    """
    Labels a media file by extension, marking per-speaker audio from an 'Audio Record' folder.

    Args:
        media_file (Path): The media file.

    Returns:
        str: e.g. 'mp4', 'm4a', 'm4a_diarized', 'wav'.
    """
    kind = media_file.suffix.lower().lstrip(".")
    if "Audio Record" in media_file.parts:
        kind += "_diarized"
    return kind


def build_rows(config_file: Path, overrides: pd.DataFrame) -> pd.DataFrame:
    """
    Probes every media file under each override and computes its day deltas.

    Args:
        config_file (Path): The path to the configuration file.
        overrides (pd.DataFrame): The output of fetch_overrides().

    Returns:
        pd.DataFrame: One row per media file (or per missing identifier), deltas in days.
    """
    rows = []
    for override in overrides.itertuples(index=False):
        identifier = Path(override.identifier)
        base = {
            "identifier": override.identifier,
            "study_id": override.study_id,
            "subject_id": override.subject_id,
            "error_codes": override.error_codes,
            "override_day": override.override_day,
            "identifier_is_dir": identifier.is_dir(),
        }
        if not identifier.exists():
            rows.append({**base, "media_file": None, "note": "identifier no longer exists"})
            continue

        media_files = list_media(identifier)
        if not media_files:
            rows.append({**base, "media_file": None, "note": "no media files"})
            continue

        for media_file in media_files:
            probed = probe_dates(config_file=config_file, media_file=media_file)
            mtime = datetime.fromtimestamp(media_file.stat().st_mtime, tz=timezone.utc)
            row = {
                **base,
                "media_file": str(media_file),
                "kind": file_kind(media_file),
                "n_media_in_identifier": len(media_files),
                "size_bytes": media_file.stat().st_size,
                "mtime_utc": mtime.isoformat(),
                "mtime_day": mtime.date(),
                "note": None if probed else "ffprobe failed",
            }
            if probed:
                row.update(probed)
                row["format_creation_day"] = utc_day(probed["format_creation_time"])
                row["stream_creation_day"] = utc_day(probed["stream_creation_time"])
            rows.append(row)

    df = pd.DataFrame(rows)
    for source in DATE_SOURCES:
        if source not in df:
            df[source] = None
        # Positive delta: the metadata day falls after the staff-confirmed day
        df[f"{source}_delta"] = [
            (s - o).days if s is not None and not pd.isna(s) else None
            for s, o in zip(df[source], df["override_day"])
        ]
    return df


def summarize(rows: pd.DataFrame) -> pd.DataFrame:
    """
    Tabulates day-level agreement per file kind and date source.

    Args:
        rows (pd.DataFrame): The output of build_rows().

    Returns:
        pd.DataFrame: n files, n with the date, exact-day and within-1-day agreement counts.
    """
    probed = rows[rows["media_file"].notna()]
    summary = []
    for kind, group in probed.groupby("kind"):
        for source in DATE_SOURCES:
            deltas = group[f"{source}_delta"].dropna().astype(int)
            summary.append({
                "kind": kind,
                "source": source,
                "n_files": len(group),
                "n_with_date": len(deltas),
                "exact_day": int((deltas == 0).sum()),
                "within_1_day": int((deltas.abs() <= 1).sum()),
                "delta_counts": deltas.value_counts().sort_index().to_dict(),
            })
    return pd.DataFrame(summary)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog=MODULE_NAME, description=__doc__)
    parser.add_argument(
        "-c", "--config", type=str, help="Path to the config file.", required=False
    )
    parser.add_argument(
        "-o", "--output", type=str, help="Per-file CSV to write.",
        default=f"{MODULE_NAME}.csv",
    )
    args = parser.parse_args()

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

    console.rule(f"[bold red]{MODULE_NAME}")

    # Without an [ffmpeg] section, ffprobe runs through singularity as in 02_metadata
    try:
        ffmpeg_bin_path = utils.config(config_file, section="ffmpeg").get("ffmpeg_bin_path")
    except ValueError:
        ffmpeg_bin_path = None
    if ffmpeg_bin_path:
        ffprobe.set_ffmpeg_bin_path(ffmpeg_bin_path)

    overrides = fetch_overrides(config_file=config_file)
    logger.info(f"Found {len(overrides)} datetime overrides")

    rows = build_rows(config_file=config_file, overrides=overrides)
    rows.to_csv(args.output, index=False)
    logger.info(f"Wrote {len(rows)} rows to {args.output}")

    with pd.option_context("display.max_colwidth", None, "display.width", 200):
        console.print(summarize(rows).to_string(index=False))
        console.print(rows["note"].value_counts(dropna=False).to_string())
