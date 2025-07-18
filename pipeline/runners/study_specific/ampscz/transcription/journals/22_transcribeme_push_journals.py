#!/usr/bin/env python
"""
Push Audio Journals to Transcribeme for transcription.
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
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import paramiko
from rich.logging import RichHandler

from pipeline import orchestrator
from pipeline.helpers import cli, db, sftp, utils
from pipeline.helpers.timer import Timer
from pipeline.models.transcribeme.transcribeme_push import TranscribemePush

MODULE_NAME = "pipeline.runners.ampscz.transcribeme_push_journals"

logger = logging.getLogger(MODULE_NAME)
logargs = {
    "level": logging.DEBUG,
    # "format": "%(asctime)s - %(process)d - %(name)s - %(levelname)s - %(message)s",
    "format": "%(message)s",
    "handlers": [RichHandler(rich_tracebacks=True)],
}
logging.basicConfig(**logargs)

console = utils.get_console()

noisy_modules: List[str] = ["paramiko.transport", "paramiko.transport.sftp", "paramiko"]
utils.silence_logs(noisy_modules=noisy_modules)

study_language_map: Dict[str, str] = {
    "PronetYA": "ENGLISH",
    "PronetMA": "SPANISH",  # Madrid
    "PronetMU": "GERMAN",  # Munich
    "PronetPV": "ITALIAN",
    "PronetSH": "MANDARIN",  # Shanghai
    "PronetSL": "KOREAN",  # Seoul
}


def get_file_to_process(
    config_file: Path, study_id: str
) -> Optional[Tuple[Path, str, str, str]]:
    """
    Get the next file to process from the database.

    This function queries the database for audio journals that have not been
    transcribed yet.

    Args:
        config_file (Path): Path to the config file.
        study_id (str): Study ID to filter the audio journals.

    Returns:
        Optional[Tuple[Path, str, str, str]]: A tuple containing the journal path,
            journal name, subject ID, and study ID. Returns None if no files are found.
    """
    query = f"""
    SELECT transcribeme.audio_qc.aqc_source_path, aj_name, subject_id, study_id
    FROM audio_journals
    LEFT JOIN transcribeme.wav_conversion ON audio_journals.aj_path = transcribeme.wav_conversion.wc_source_path
    LEFT JOIN transcribeme.audio_qc ON
        transcribeme.wav_conversion.wc_destination_path = transcribeme.audio_qc.aqc_source_path
    WHERE transcribeme.audio_qc.aqc_passed IS TRUE AND
        transcribeme.audio_qc.aqc_source_path NOT IN (
            SELECT transcription_source_path
            FROM transcribeme.transcribeme_push
        ) AND
        study_id = '{study_id}'
    ORDER BY aj_name
    LIMIT 1
    """

    result_df = db.execute_sql(
        config_file=config_file,
        query=query,
    )

    if result_df.empty:
        return None

    journal_path = Path(result_df.iloc[0]["aqc_source_path"])
    journal_name = result_df.iloc[0]["aj_name"]
    subject_id = result_df.iloc[0]["subject_id"]
    study_id = result_df.iloc[0]["study_id"]

    return journal_path, journal_name, subject_id, study_id


def get_subject_journals_root(
    config_file: Path, subject_id: str, study_id: str
) -> Path:
    """
    Get the root directory for the subject's journals.

    Args:
        config_file (Path): Path to the config file.
        subject_id (str): Subject ID.
        study_id (str): Study ID.

    Returns:
        Path: Path to the subject's journals root directory.
    """
    data_root = orchestrator.get_data_root(config_file=config_file, enforce_real=True)
    subject_root = data_root / "PROTECTED" / study_id / "processed" / subject_id
    subject_journals_root = subject_root / "phone" / "audio_journals"

    return subject_journals_root


def construct_transcript_destination_path(
    subject_journals_root: Path, journal_name: str
) -> Path:
    """
    Construct the path for the transcript destination file.

    This is where the transcript will be saved after transcription.

    Args:
        subject_journals_root (Path): Path to the subject's journals root directory.
        journal_name (str): Name of the journal.

    Returns:
        Path: Path to the transcript destination file.
    """
    transcription_destination_path = (
        subject_journals_root
        / "transcripts"
        / f"{journal_name.replace('-', '_').replace('session', 'submission')}.txt"
    )
    if not transcription_destination_path.parent.exists():
        transcription_destination_path.parent.mkdir(parents=True, exist_ok=True)
    return transcription_destination_path


def construct_sftp_upload_path(journal_path: Path, source_language: str) -> str:
    """
    Returns the SFTP upload path for the journal.

    Args:
        journal_path (path): Path to the journal file.
        source_language (str): Source language of the journal.

    Returns:
        str: SFTP upload path for the journal.
    """
    file_name = journal_path.name

    # Append source language to the file name, before the file extension
    file_name = file_name.replace(
        ".wav", f"-{source_language.lower()}.wav"
    )  # Ensure the file extension is preserved

    file_name = file_name.replace("-", "_")

    sftp_upload_path = f"/audio/{file_name}"
    return sftp_upload_path


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog=MODULE_NAME, description="Run Quick QC on video files."
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

    config_params = utils.config(config_file, section="general")
    studies = orchestrator.get_studies(config_file=config_file)

    COUNTER = 0

    logger.info("Starting transcribeme_push_journals loop...", extra={"markup": True})
    study_id = studies[0]
    logger.info(f"Using study: {study_id}")

    while True:
        # Get file to process
        file_to_process = get_file_to_process(
            config_file=config_file, study_id=study_id
        )

        if file_to_process is None:
            if study_id == studies[-1]:
                # Log if any files were processed
                if COUNTER > 0:
                    orchestrator.log(
                        config_file=config_file,
                        module_name=MODULE_NAME,
                        message=f"Pushed {COUNTER} audio journals to Transcribeme.",
                    )
                    COUNTER = 0

                # Exit if all studies are done
                logger.info(
                    "No more audio journals to process. Exiting.",
                    extra={"markup": True},
                )
                sys.exit(0)
            else:
                study_id = studies[studies.index(study_id) + 1]
                logger.info(f"Switching to study: {study_id}", extra={"markup": True})
                continue

        COUNTER += 1
        journal_path, journal_name, subject_id, study_id = file_to_process
        source_language = study_language_map.get(study_id, "ENGLISH")
        logger.info(
            f"Handling Journal: {journal_path} [{source_language}]",
            extra={"markup": True},
        )

        subject_journals_root = get_subject_journals_root(
            config_file=config_file, subject_id=subject_id, study_id=study_id
        )

        # Upload to Transcribeme
        logger.info(
            f"[green]Uploading {journal_path} to Transcribeme SFTP server...",
            extra={"markup": True},
        )
        transcription_destination_path = construct_transcript_destination_path(
            subject_journals_root=subject_journals_root,
            journal_name=journal_name,
        )
        sftp_upload_path = construct_sftp_upload_path(
            journal_path=journal_path,
            source_language=source_language,
        )

        sftp_credentials = sftp.get_sftp_credentials(
            config_file=config_file, sftp_credentials_name="transcribeme_sftp"
        )
        sftp_hostname = sftp_credentials["hostname"]
        sftp_username = sftp_credentials["username"]
        sftp_password = sftp_credentials["password"]
        sftp_port = int(sftp_credentials["port"])

        with Timer() as timer:
            with paramiko.SSHClient() as ssh:
                ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                ssh.connect(
                    hostname=sftp_hostname,
                    username=sftp_username,
                    password=sftp_password,
                    port=sftp_port,
                    look_for_keys=False,
                    timeout=120,
                )

                with ssh.open_sftp() as sftp_client:
                    try:
                        sftp_client.stat(sftp_upload_path)
                        logger.warning(
                            f"File {sftp_upload_path} exists on SFTP server."
                        )
                        sys.exit(0)
                    except FileNotFoundError:
                        logger.info(
                            f"File {sftp_upload_path} does not exist on SFTP server."
                        )

                    # Upload the file
                    sftp_client.put(journal_path, sftp_upload_path)
                    logger.info(f"File {sftp_upload_path} uploaded successfully.")

        push_obj = TranscribemePush(
            transcription_source_path=journal_path,
            source_language=source_language,
            transcription_destination_path=transcription_destination_path,
            sftp_upload_path=sftp_upload_path,
            sftp_upload_duration_s=timer.duration,  # type: ignore
            sftp_upload_timestamp=datetime.now(),
        )

        insert_query = [push_obj.to_sql()]

        db.execute_queries(
            config_file=config_file,
            queries=insert_query,
        )

        logger.info(
            f"Inserted {journal_path} to database.",
            extra={"markup": True},
        )
