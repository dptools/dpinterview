"""
Manual audio QC override.

Reviewers can mark a file that failed automated audio QC
(transcribeme.audio_qc.aqc_passed = FALSE) as manually approved
(aqc_override = TRUE) from the dashboard. This module relocates such a file
from `rejected_audio/` back into `pending_audio/` the next time a
transcribeme push runner looks for work, so it flows through the rest of the
pipeline exactly as if it had passed QC.

The file's path is a primary/foreign key chain across `files`,
`transcribeme.wav_conversion`, and `transcribeme.audio_qc`, and none of
those foreign keys are DEFERRABLE - so the path can't be renamed in place
across tables in one transaction. Instead this deletes the old rows and
re-inserts fresh ones at the new path, carrying forward the original QC
metrics/fail_reasons/timestamps (aqc_passed stays FALSE - it's a factual
record of what the automated check found; aqc_override is what lets the
file through).
"""

import logging
from pathlib import Path
from typing import Optional

import pandas as pd

from pipeline.helpers import db
from pipeline.models.files import File
from pipeline.models.transcribeme.audio_qc import AudioQC
from pipeline.models.transcribeme.wav_conversion import WavConversion

logger = logging.getLogger(__name__)


def _fetch_current_row(wav_path: Path, config_file: Path) -> Optional[pd.Series]:
    query = f"""
        SELECT
            wc.wc_source_path, wc.wc_duration_s, wc.wc_timestamp,
            aqc.aqc_passed, aqc.aqc_metrics, aqc.aqc_fail_reasons,
            aqc.aqc_duration_s, aqc.aqc_timestamp, aqc.aqc_override
        FROM transcribeme.wav_conversion wc
        JOIN transcribeme.audio_qc aqc ON aqc.aqc_source_path = wc.wc_destination_path
        WHERE wc.wc_destination_path = '{db.santize_string(wav_path)}'
    """
    result_df = db.execute_sql(config_file=config_file, query=query)
    if result_df.empty:
        return None
    return result_df.iloc[0]


def relocate_if_overridden(wav_path: Path, config_file: Path) -> Path:
    """
    If `wav_path` is a manually-overridden failed-QC file still sitting in
    `rejected_audio/`, move it to `pending_audio/` and re-point the
    files/wav_conversion/audio_qc rows at the new path, preserving the
    original QC metrics/fail_reasons/timestamps.

    Idempotent: files that already passed QC, aren't overridden, or have
    already been relocated by a previous run are returned unchanged.

    Args:
        wav_path (Path): Current path of the converted WAV file, i.e. the
            current transcribeme.audio_qc.aqc_source_path /
            transcribeme.wav_conversion.wc_destination_path.
        config_file (Path): Path to the config file.

    Returns:
        Path: The file's path after relocation (or `wav_path` unchanged if
            no relocation was needed).
    """
    row = _fetch_current_row(wav_path=wav_path, config_file=config_file)
    if row is None:
        return wav_path

    if bool(row["aqc_passed"]) or not bool(row["aqc_override"]):
        return wav_path

    if wav_path.parent.name != "rejected_audio":
        # Already relocated by a previous run.
        return wav_path

    new_path = wav_path.parent.parent / "pending_audio" / wav_path.name
    new_path.parent.mkdir(parents=True, exist_ok=True)

    if new_path.exists():
        raise FileExistsError(
            f"Cannot relocate overridden audio QC file: {new_path} already exists."
        )

    logger.info(
        f"Relocating manually-overridden audio QC file {wav_path} -> {new_path}",
        extra={"markup": True},
    )
    wav_path.rename(new_path)

    new_file = File(file_path=new_path)
    new_wav_conversion = WavConversion(
        wc_source_path=Path(row["wc_source_path"]),
        wc_destination_path=new_path,
        wc_duration_s=row["wc_duration_s"],
    )
    new_wav_conversion.wc_timestamp = row["wc_timestamp"]
    new_audio_qc = AudioQC(
        aqc_source_path=new_path,
        aqc_passed=False,
        aqc_metrics=row["aqc_metrics"],
        aqc_fail_reasons=row["aqc_fail_reasons"],
        aqc_duration_s=row["aqc_duration_s"],
        aqc_timestamp=row["aqc_timestamp"],
    )

    sanitized_old_path = db.santize_string(wav_path)
    sanitized_new_path = db.santize_string(new_path)

    queries = [
        f"DELETE FROM transcribeme.audio_qc WHERE aqc_source_path = '{sanitized_old_path}';",
        f"DELETE FROM transcribeme.wav_conversion WHERE wc_destination_path = '{sanitized_old_path}';",
        f"DELETE FROM files WHERE file_path = '{sanitized_old_path}';",
        new_file.to_sql(),
        new_wav_conversion.to_sql(),
        new_audio_qc.to_sql(),
        f"UPDATE transcribeme.audio_qc SET aqc_override = TRUE "
        f"WHERE aqc_source_path = '{sanitized_new_path}';",
    ]

    db.execute_queries(
        config_file=config_file,
        queries=queries,
        show_commands=False,
        failure_stage="pipeline.core.audio_qc_override",
        failure_identifier=str(new_path),
        failure_identifier_type="file_path",
    )

    return new_path
