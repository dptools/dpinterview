#!/usr/bin/env python
"""
Runs the ProNET AMPSCZ crawlers in sequence against a single config file.

Each crawler below currently runs as its own independent cron entry. This
just chains the same scripts, in their dependency order (subjects before
interviews/journals before form data before expected interviews), stopping
at the first failure instead of letting downstream stages run against stale
or incomplete data.

Each stage runs as its own subprocess rather than being imported in-process:
every crawler script does its own module-level logging setup (logging.
basicConfig) and sys.path bootstrap, so importing several of them into one
process risks duplicate/conflicting logging handlers. A fresh process per
stage also matches how these already run under cron today.
"""

import subprocess
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

from rich.logging import RichHandler

from pipeline.helpers import db, utils

MODULE_NAME = "run_pronet_crawlers"

logger = logging.getLogger(MODULE_NAME)
logargs = {
    "level": logging.DEBUG,
    "format": "%(message)s",
    "handlers": [RichHandler(rich_tracebacks=True)],
}
logging.basicConfig(**logargs)

console = utils.get_console()

CRAWLERS_DIR = Path(__file__).resolve().parent

CRAWLER_SCRIPTS = [
    CRAWLERS_DIR / "1_import_study_metadata.py",
    CRAWLERS_DIR / "2_import_interview_files.py",
    CRAWLERS_DIR / "3_import_transcripts.py",
    CRAWLERS_DIR / "4_import_audio_journals.py",
    CRAWLERS_DIR / "5_import_journal_transcripts.py",
    CRAWLERS_DIR / "6_import_data_dictionary.py",
    CRAWLERS_DIR / "pronet" / "7_import_form_data.py",
    CRAWLERS_DIR / "8_import_expected_interviews.py",
]


def run_crawler(script_path: Path, config_file: Path) -> int:
    """
    Runs a single crawler script as a subprocess against config_file.

    Args:
        script_path (Path): The crawler script to run.
        config_file (Path): The config file to pass to it via -c.

    Returns:
        int: The subprocess's exit code.
    """
    logger.info(f"[bold cyan]Running {script_path.name}...", extra={"markup": True})

    result = subprocess.run(
        [sys.executable, str(script_path), "-c", str(config_file)],
        check=False,
    )

    return result.returncode


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog=MODULE_NAME,
        description="Run the ProNET AMPSCZ crawlers in sequence.",
    )
    parser.add_argument(
        "-c", "--config", type=str, help="Path to the config file.", required=False, default='pronet.ampscz.config.ini'
    )

    args = parser.parse_args()

    if args.config:
        config_file = Path(args.config).resolve()
        if not config_file.exists():
            logger.error(f"Error: Config file '{config_file}' does not exist.")
            sys.exit(1)
    else:
        config_file = utils.get_config_file_path()

    console.rule(f"[bold red]{MODULE_NAME}")
    logger.info(f"Using config file: {config_file}")

    for script_path in CRAWLER_SCRIPTS:
        if not script_path.exists():
            logger.error(f"[bold red]Crawler script not found: {script_path}")
            sys.exit(1)

        returncode = run_crawler(script_path=script_path, config_file=config_file)

        if returncode != 0:
            logger.error(
                f"[bold red]{script_path.name} failed (exit code {returncode}); "
                f"stopping before running downstream crawlers.",
                extra={"markup": True},
            )
            db.record_failure(
                config_file=config_file,
                stage=MODULE_NAME,
                error_code="crawler_stage_failed",
                identifier=script_path.stem,
                error=f"{script_path.name} exited with code {returncode}",
                identifier_type="batch",
            )
            sys.exit(returncode)

        logger.info(f"[bold green]{script_path.name} completed.", extra={"markup": True})

    logger.info(
        "[bold green]All ProNET crawlers completed successfully.",
        extra={"markup": True},
    )
