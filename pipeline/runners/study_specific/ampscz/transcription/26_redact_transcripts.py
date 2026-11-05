#!/usr/bin/env python
"""
Redacted transcripts from Transcribeme
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
import re
from datetime import datetime
from typing import List, Optional

from rich.logging import RichHandler

from pipeline.helpers import cli, db, utils
from pipeline.models.transcribeme.redacted_transcripts import RedactedTranscript

MODULE_NAME = "pipeline.runners.ampscz.transcription.redact_transcripts"

logger = logging.getLogger(MODULE_NAME)
logargs = {
    "level": logging.DEBUG,
    # "format": "%(asctime)s - %(process)d - %(name)s - %(levelname)s - %(message)s",
    "format": "%(message)s",
    "handlers": [RichHandler(rich_tracebacks=True)],
}
logging.basicConfig(**logargs)

console = utils.get_console()

noisy_modules: List[str] = []
utils.silence_logs(noisy_modules=noisy_modules)


def get_transcript_pending_redaction(config_file: Path) -> Optional[Path]:
    """
    Get the next transcript that needs redaction
    (that has not been redacted yet).

    Args:
        config_file (Path): Path to the configuration file.

    Returns:
        Optional[Path]: Path to the unredacted transcript if found, else None.
    """
    query = """
    SELECT transcribeme_pull.transcription_destination_path
    FROM transcribeme.transcribeme_pull
    LEFT JOIN transcribeme.redacted_transcripts
    ON transcribeme_pull.transcription_destination_path = redacted_transcripts.unredacted_transcript_path
    WHERE redacted_transcripts.unredacted_transcript_path IS NULL
    ORDER BY RANDOM()
    LIMIT 1
    """

    result = db.fetch_record(
        query=query,
        config_file=config_file,
    )

    if result:
        unredacted_transcript_path = Path(result)
        return unredacted_transcript_path

    return None


def construct_redacted_transcript_path(
    unredacted_transcript_path: Path,
) -> Path:
    """
    Construct the redacted transcript path from the unredacted transcript path.

    Args:
        unredacted_transcript_path (Path): Path to the unredacted transcript.
    Returns:
        Path: Path to the redacted transcript.
    """
    parts = unredacted_transcript_path.parts
    protected_index = parts.index("PROTECTED")
    general_parts = list(parts)
    general_parts[protected_index] = "GENERAL"
    redacted_transcript_path = Path(*general_parts)
    redacted_transcript_path = redacted_transcript_path.with_name(
        redacted_transcript_path.stem + "_REDACTED" + redacted_transcript_path.suffix
    )
    return redacted_transcript_path


def redact_transcibeme_transcript(
    unredacted_transcript_path: Path,
    redacted_transcript_path: Path,
) -> None:
    """
    Redact sensitive information from a Transcribeme transcript.

    Args:
        unredacted_transcript_path (Path): Path to the unredacted transcript.
        redacted_transcript_path (Path): Path to save the redacted transcript.

    Returns:
        None

    Note:
        This function assumes that sensitive information in Transcribeme
        transcripts is always surrounded by '{' and '}'.
    """
    transcript_text = unredacted_transcript_path.read_text()

    # TranscribeMe convention is to use '{' and '}' to surround redacted text
    redacted_text = re.sub(r"\{.*?\}", "[REDACTED]", transcript_text)

    redacted_transcript_path.write_text(redacted_text)
    return None


def handle_redaction(
    unredacted_transcript_path: Path,
) -> RedactedTranscript:
    """
    Handle the redaction of a Transcribeme transcript.

    Args:
        unredacted_transcript_path (Path): Path to the unredacted transcript.
    Returns:
        RedactedTranscript: Instance containing details of the redacted transcript.
    """
    redacted_transcript_path = construct_redacted_transcript_path(
        unredacted_transcript_path=unredacted_transcript_path,
    )
    redacted_transcript_path.parent.mkdir(parents=True, exist_ok=True)

    redact_transcibeme_transcript(
        unredacted_transcript_path=unredacted_transcript_path,
        redacted_transcript_path=redacted_transcript_path,
    )

    redacted_transcript = RedactedTranscript(
        unredacted_transcript_path=unredacted_transcript_path,
        redacted_transcript_path=redacted_transcript_path,
        redaction_timestamp=datetime.now(),
    )

    return redacted_transcript


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog=MODULE_NAME, description="Redact Transcripts from Transcribeme"
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

    COUNTER = 0

    logger.info(
        "[bold green]Starting transcript redaction loop...", extra={"markup": True}
    )

    while True:
        unredacted_transcript_path = get_transcript_pending_redaction(
            config_file=config_file,
        )

        if unredacted_transcript_path is None:
            logger.info(
                "No transcripts pending redaction. Exiting redaction loop."
            )
            break

        logger.info(
            f"Redacting transcript: {unredacted_transcript_path}"
        )
        redacted_transcript = handle_redaction(
            unredacted_transcript_path=unredacted_transcript_path,
        )
        logger.info(
            f"Redacted transcript saved to: {redacted_transcript.redacted_transcript_path}"
        )

        db.execute_queries(
            config_file=config_file,
            queries=[redacted_transcript.to_sql()],
            show_commands=True,
        )

        COUNTER += 1
        # break  # Remove this break to enable continuous processing

    if COUNTER > 0:
        message = (
            f"[bold green]Transcript redaction loop completed. "
            f"Total transcripts redacted: {COUNTER}"
        )
        logger.info(message)
