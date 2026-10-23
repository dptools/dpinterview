#!/usr/bin/env python
"""
Extract eGeMAPSv02 per-frame acoustic features from QC'd combined interview audio.

Each recording gets a per-frame LLD CSV, recorded in `acoustics.lld_extraction`.
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
import logging
from dataclasses import dataclass

import pandas as pd
from rich.logging import RichHandler

from pipeline import orchestrator
from pipeline.core.acoustics import AcousticRecording
from pipeline.helpers import cli, db, utils
from pipeline.helpers.timer import Timer
from pipeline.models.acoustics.lld_extraction import TABLE_NAME, LldExtraction

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
    """A QC'd recording with no extraction recorded yet.

    Attributes:
        source_path: The original recording, the stable identity of the extraction.
        audio_path: Where its QC'd WAV currently is (`completed_audio/` once transcribed).
        name: Interview name, used as the output file stem.
        output_dir: Directory the outputs are written into.
    """

    source_path: Path
    audio_path: Path
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
        subject_root = data_root / 'PROTECTED' / row['study_id'] / 'processed' / row['subject_id']
        output_dir = subject_root / 'interviews' / row['interview_type'] / 'acoustics'

        return cls(
            source_path=Path(row['source_path']),
            audio_path=Path(row['audio_path']),
            name=row['name'],
            output_dir=output_dir,
        )


def get_next_recording(
    config_file: Path,
    data_root: Path,
    skipped: set[Path],
) -> PendingRecording | None:
    """Fetch one QC'd combined interview recording not yet extracted or skipped.

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
        skipped_clause = f'AND wc.wc_source_path NOT IN ({skipped_list})'

    query = f"""
        SELECT DISTINCT ON (wc.wc_source_path)
            wc.wc_source_path AS source_path,
            COALESCE(p.completed_audio_file_path, q.aqc_source_path) AS audio_path,
            i.interview_name AS name,
            i.study_id,
            i.subject_id,
            i.interview_type
        FROM transcribeme.audio_qc AS q
        JOIN transcribeme.wav_conversion AS wc
            ON wc.wc_destination_path = q.aqc_source_path
        JOIN public.interview_files AS f
            ON f.interview_file = wc.wc_source_path
            AND f.interview_file_tags LIKE '%%combined%%'
        JOIN public.interview_parts AS ip
            ON ip.interview_path = f.interview_path
        JOIN public.interviews AS i
            ON i.interview_name = ip.interview_name
        LEFT JOIN transcribeme.transcribeme_pull AS p
            ON p.sftp_download_path = q.aqc_source_path
        WHERE (q.aqc_passed IS TRUE OR q.aqc_override IS TRUE)
            AND wc.wc_source_path NOT IN (SELECT lld_source_path FROM {TABLE_NAME})
            {skipped_clause}
        ORDER BY wc.wc_source_path, p.sftp_download_timestamp DESC NULLS LAST
        LIMIT 1;
    """

    result_df = db.execute_sql(config_file=config_file, query=query)
    if result_df.empty:
        return None

    return PendingRecording.from_row(row=result_df.iloc[0], data_root=data_root)


def extract_recording(config_file: Path, pending: PendingRecording) -> None:
    """Extract features for one recording, write its outputs, and record it in the database.

    Args:
        config_file: Path to the config file.
        pending: The recording to extract.
    """
    with Timer() as timer:
        recording = AcousticRecording(audio_path=pending.audio_path)
        outputs = recording.save(output_dir=pending.output_dir, name=pending.name)

    extraction = LldExtraction(
        lld_source_path=pending.source_path,
        lld_audio_path=pending.audio_path,
        lld_output_path=outputs.lld_path,
        lld_num_frames=len(recording.lld),
        lld_duration_s=timer.duration,  # type: ignore[arg-type]
    )
    db.execute_queries(
        config_file=config_file,
        queries=[extraction.to_sql()],
        show_commands=False,
        failure_stage=MODULE_NAME,
        failure_identifier=str(pending.source_path),
        failure_identifier_type='file_path',
    )
    logger.info(f'Wrote {extraction.lld_num_frames} frames to {outputs.lld_path}')


def get_config_file() -> Path:
    """Resolve the config file from the command line, or confirm the default.

    Returns:
        Path to an existing config file.
    """
    parser = argparse.ArgumentParser(
        prog=MODULE_NAME,
        description="Extract eGeMAPSv02 features from QC'd interview audio.",
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
                    message=f'Extracted acoustic features for {counter} recordings.',
                )
                counter = 0
            orchestrator.snooze(config_file=config_file)
            continue

        if not pending.audio_path.exists():
            logger.warning(f'Skipping missing audio file: {pending.audio_path}')
            db.record_failure(
                config_file=config_file,
                stage=MODULE_NAME,
                error_code='missing_file',
                identifier=str(pending.source_path),
                error='Audio file not found for acoustic extraction',
            )
            skipped.add(pending.source_path)
            continue

        logger.info(f'Extracting: {pending.audio_path}')
        try:
            extract_recording(config_file=config_file, pending=pending)
        except Exception as error:  # pylint: disable=broad-except
            # Contains opensmile/decoder errors to one file; a failed DB write still exits.
            logger.exception(f'Acoustic extraction failed for {pending.audio_path}')
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
