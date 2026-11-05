#!/usr/bin/env python
"""
Extract eGeMAPSv02 acoustic features from single-speaker subject recordings.

Covers audio journals and interview audio files of the subject alone. Each recording gets a
hard-failure QC verdict in `acoustics.audio_qc`; those passing get a per-frame LLD CSV and a
functionals JSON, recorded in `acoustics.lld_extraction`.
"""

import sys
from pathlib import Path

file = Path(__file__).resolve()
parent = file.parent
ROOT = None
for parent in file.parents:
    if parent.name == 'dpinterview':
        ROOT = parent
sys.path.append(str(ROOT))

# remove current directory from path
try:
    sys.path.remove(str(parent))
except ValueError:
    pass

import argparse
import hashlib
import logging
from dataclasses import dataclass

import numpy as np
import pandas as pd
from rich.logging import RichHandler

from pipeline import orchestrator
from pipeline.core import acoustics
from pipeline.core.acoustic_qc import SignalQc
from pipeline.helpers import cli, db, ffmpeg, utils
from pipeline.helpers.timer import Timer
from pipeline.models.acoustics.audio_qc import TABLE_NAME as QC_TABLE_NAME
from pipeline.models.acoustics.audio_qc import AudioQc
from pipeline.models.acoustics.lld_extraction import LldExtraction, SourceKind

MODULE_NAME = 'pipeline.runners.ampscz.extract_acoustics'

logger = logging.getLogger(MODULE_NAME)
logging.basicConfig(
    level=logging.DEBUG,
    format='%(message)s',
    handlers=[RichHandler(rich_tracebacks=True)],
)

console = utils.get_console()


@dataclass(frozen=True)
class PendingRecording:
    """A subject-only recording with no QC verdict yet.

    Attributes:
        source_path: The original recording, the identity of its QC verdict and extraction.
        kind: Interview or audio journal.
        name: Output file stem.
        output_dir: Directory the outputs are written into.
    """

    source_path: Path
    kind: SourceKind
    name: str
    output_dir: Path

    @classmethod
    def from_row(cls, row: pd.Series, data_root: Path) -> 'PendingRecording':
        """Build from a selection-query row, placing outputs under `processed/`.

        Args:
            row: One row of the query in `get_next_recording`.
            data_root: The real (not fake) data root.

        Returns:
            The pending recording.
        """
        source_path = Path(row['source_path'])
        subject_root = data_root / 'PROTECTED' / row['study_id'] / 'processed' / row['subject_id']
        if row['source_kind'] == 'journal':
            output_dir = subject_root / 'phone' / 'audio_journals' / 'acoustics'
            name = row['name']
        else:
            output_dir = subject_root / 'interviews' / row['interview_type'] / 'acoustics'
            # Zoom file stems carry display names, so a path hash keeps names unique and stable.
            path_hash = hashlib.sha1(str(source_path).encode()).hexdigest()[:8]
            name = f"{row['name']}_subject_{path_hash}"

        return cls(
            source_path=source_path,
            kind=row['source_kind'],
            name=name,
            output_dir=output_dir,
        )


def get_next_recording(
    config_file: Path,
    data_root: Path,
    skipped: set[Path],
) -> PendingRecording | None:
    """Fetch one journal or subject interview file with no QC verdict, not skipped this run.

    Args:
        config_file: Path to the config file.
        data_root: The real (not fake) data root.
        skipped: Source paths that failed earlier in this process.

    Returns:
        The next recording, or None if there is no work.
    """
    skipped_clause = ''
    if skipped:
        skipped_list = ', '.join(f"'{db.santize_string(path)}'" for path in skipped)
        skipped_clause = f'AND c.source_path NOT IN ({skipped_list})'

    # Subject files are those the importer tagged `participant` by finding the subject ID in the
    # name; its fallback tag (a lone unclassified file) is excluded.
    query = f"""
        WITH candidates AS (
            SELECT
                aj.aj_path AS source_path,
                'journal' AS source_kind,
                aj.aj_name AS name,
                aj.study_id,
                aj.subject_id,
                NULL::TEXT AS interview_type
            FROM public.audio_journals AS aj
            UNION ALL
            SELECT
                f.interview_file,
                'interview',
                i.interview_name,
                i.study_id,
                i.subject_id,
                i.interview_type
            FROM public.interview_files AS f
            JOIN public.interview_parts AS ip ON ip.interview_path = f.interview_path
            JOIN public.interviews AS i ON i.interview_name = ip.interview_name
            WHERE f.interview_file_tags LIKE '%%participant%%'
                AND f.interview_file LIKE '%%' || i.subject_id || '%%'
                AND f.ignored IS NOT TRUE
                AND ip.is_primary IS TRUE
                AND ip.is_duplicate IS NOT TRUE
        )
        SELECT c.*
        FROM candidates AS c
        WHERE NOT EXISTS (
            SELECT 1 FROM {QC_TABLE_NAME} AS q WHERE q.qc_source_path = c.source_path
        )
            {skipped_clause}
        LIMIT 1;
    """

    result_df = db.execute_sql(config_file=config_file, query=query)
    if result_df.empty:
        return None

    return PendingRecording.from_row(row=result_df.iloc[0], data_root=data_root)


def extract_features(signal: np.ndarray, pending: PendingRecording) -> LldExtraction:
    """Extract and write one recording's features.

    Args:
        signal: The decoded signal, already through QC.
        pending: The recording it came from.

    Returns:
        The extraction record, not yet written to the database.
    """
    with Timer() as timer:
        features = acoustics.AcousticFeatures(signal=signal)
        outputs = features.save(output_dir=pending.output_dir, name=pending.name)

    return LldExtraction(
        lld_source_path=pending.source_path,
        lld_source_kind=pending.kind,
        lld_output_path=outputs.lld_path,
        lld_summary_path=outputs.summary_path,
        lld_num_frames=len(features.lld),
        lld_duration_s=timer.duration,  # type: ignore[arg-type]
    )


def process_recording(config_file: Path, pending: PendingRecording) -> None:
    """QC one recording, extract its features if it passes, and record both in one transaction.

    Args:
        config_file: Path to the config file.
        pending: The recording to process.
    """
    try:
        signal = acoustics.load_signal(source_path=pending.source_path)
        qc = SignalQc.from_signal(signal)
    except acoustics.UnreadableAudioError:
        signal = None
        qc = SignalQc.unreadable()

    verdict = AudioQc(
        qc_source_path=pending.source_path,
        qc_passed=qc.passed,
        qc_fail_reasons=qc.fail_reasons,
        qc_audio_duration_s=qc.duration_s,
        qc_peak_amplitude=qc.peak_amplitude,
    )
    queries = [verdict.to_sql()]

    extraction = None
    if signal is not None and qc.passed:
        extraction = extract_features(signal=signal, pending=pending)
        queries.append(extraction.to_sql())

    db.execute_queries(
        config_file=config_file,
        queries=queries,
        show_commands=False,
        failure_stage=MODULE_NAME,
        failure_identifier=str(pending.source_path),
        failure_identifier_type='file_path',
    )

    if extraction is not None:
        logger.info(f'Wrote {extraction.lld_num_frames} frames to {extraction.lld_output_path}')
    else:
        logger.warning(f'Failed QC ({", ".join(qc.fail_reasons)}): {pending.source_path}')


def prepare_extraction(config_file: Path) -> None:
    """Point temp files and FFmpeg at their configured locations, then check opensmile.

    Args:
        config_file: Path to the config file.
    """
    orchestrator.redirect_temp_dir(config_file=config_file)
    try:
        ffmpeg_bin_path = utils.config(config_file, section='ffmpeg').get('ffmpeg_bin_path')
    except ValueError:  # no [ffmpeg] section: use ffmpeg from PATH
        ffmpeg_bin_path = None
    if ffmpeg_bin_path:
        ffmpeg.set_ffmpeg_bin_path(ffmpeg_bin_path)

    acoustics.verify_single_pass()


def get_config_file() -> Path:
    """Resolve the config file from the command line, or confirm the default.

    Returns:
        Path to an existing config file.
    """
    parser = argparse.ArgumentParser(
        prog=MODULE_NAME,
        description='Extract eGeMAPSv02 features from single-speaker subject recordings.',
    )
    parser.add_argument('-c', '--config', type=str, help='Path to the config file.')
    args = parser.parse_args()

    if args.config:
        config_file = Path(args.config).resolve()
        if not config_file.exists():
            logger.error(f"Error: Config file '{config_file}' does not exist.")
            sys.exit(1)
        return config_file

    if cli.confirm_action('Using default config file.'):
        return utils.get_config_file_path()
    sys.exit(1)


def main() -> None:
    config_file = get_config_file()
    utils.configure_logging(config_file=config_file, module_name=MODULE_NAME, logger=logger)

    console.rule(f'[bold red]{MODULE_NAME}')
    logger.info(f'Using config file: {config_file}')

    data_root = orchestrator.get_data_root(config_file=config_file, enforce_real=True)
    prepare_extraction(config_file=config_file)

    # Failures are retried on restart, not every loop; the ledger keeps the record.
    skipped: set[Path] = set()
    counter = 0

    while True:
        pending = get_next_recording(
            config_file=config_file,
            data_root=data_root,
            skipped=skipped,
        )

        if pending is None:
            if counter > 0:
                orchestrator.log(
                    config_file=config_file,
                    module_name=MODULE_NAME,
                    message=f'Processed {counter} recordings for acoustic extraction.',
                )
                counter = 0
            orchestrator.snooze(config_file=config_file)
            continue

        if not pending.source_path.exists():
            logger.warning(f'Skipping missing audio file: {pending.source_path}')
            db.record_failure(
                config_file=config_file,
                stage=MODULE_NAME,
                error_code='missing_file',
                identifier=str(pending.source_path),
                error='Audio file not found for acoustic extraction',
            )
            skipped.add(pending.source_path)
            continue

        logger.info(f'Processing ({pending.kind}): {pending.source_path}')
        try:
            process_recording(config_file=config_file, pending=pending)
        except Exception as error:  # pylint: disable=broad-except
            # Contains opensmile errors to one file; nothing is recorded, so it retries on restart.
            logger.exception(f'Acoustic extraction failed for {pending.source_path}')
            db.record_failure(
                config_file=config_file,
                stage=MODULE_NAME,
                error_code='acoustic_extraction_failed',
                identifier=str(pending.source_path),
                error=error,
            )
            skipped.add(pending.source_path)
            continue

        counter += 1


if __name__ == '__main__':
    main()
