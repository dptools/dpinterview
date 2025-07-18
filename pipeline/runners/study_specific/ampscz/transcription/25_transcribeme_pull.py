#!/usr/bin/env python
"""
Pull files that have been delivered from Transcribeme.

This script will only pull files previously submitted by this
pipeline.
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
import time
from datetime import datetime
from typing import List, Optional, Tuple

import paramiko
from rich.logging import RichHandler

from pipeline.helpers import cli, db, sftp, utils
from pipeline.helpers.timer import Timer
from pipeline.models.transcribeme.transcribeme_pull import TranscribemePull

MODULE_NAME = "pipeline.runners.ampscz.transcribeme_pull"

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


def get_pending_downloads_local(
    config_file: Path,
) -> Optional[List[Tuple[Path, Path, Path]]]:
    """
    Get the list of pending downloads from the database.

    These are files that had been uploaded to the SFTP server but
    their deliverables (transcripts), are yet to be downloaded.

    Args:
        config_file (Path): Path to the config file.

    Returns:
        Optional[List[Tuple[Path, Path, Path]]]: List of tuples
            containing the transcription source path, SFTP upload path,
            and transcription destination path.

        transcription_source_path: Local Audio file that was uploaded for transcription.
        sftp_upload_path: Remote Path to the file on the SFTP server.
        transcription_destination_path: Intended local path for the (to-be) downloaded  transcript.
    """
    query = """
        SELECT
            transcription_source_path,
            sftp_upload_path,
            transcription_destination_path
        FROM
            transcribeme.transcribeme_push
        WHERE transcription_destination_path NOT IN (
            SELECT transcription_destination_path
            FROM transcribeme.transcribeme_pull
        )
    """

    result_df = db.execute_sql(
        config_file=config_file,
        query=query,
    )

    if result_df.empty:
        return None

    # Convert the result to a list of tuples
    pending_downloads: List[Tuple[Path, Path, Path]] = []

    for _, row in result_df.iterrows():
        transcription_source_path = Path(row["transcription_source_path"])
        sftp_upload_path = Path(row["sftp_upload_path"])
        transcription_destination_path = Path(row["transcription_destination_path"])

        pending_downloads.append(
            (
                transcription_source_path,
                sftp_upload_path,
                transcription_destination_path,
            )
        )

    return pending_downloads


def get_files_awaiting_download(config_file: Path) -> List[Path]:
    """
    Get the list of files awaiting download from the SFTP server.

    These are files that have been delivered by Transcribeme but
    have not yet been downloaded by the pipeline.

    Args:
        config_file (Path): Path to the config file.

    Returns:
        List[Path]: List of remote paths to the files awaiting download.
    """
    sftp_credentials = sftp.get_sftp_credentials(
        config_file=config_file, sftp_credentials_name="transcribeme_sftp"
    )
    # sftp_hostname = sftp_credentials["hostname"]
    # sftp_username = sftp_credentials["username"]
    # sftp_password = sftp_credentials["password"]
    # sftp_port = int(sftp_credentials["port"])

    awaiting_downloads: List[Path] = []
    with paramiko.SSHClient() as ssh:
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(
            # hostname=sftp_hostname,
            # username=sftp_username,
            # password=sftp_password,
            # port=sftp_port,
            look_for_keys=False,
            timeout=120,
            **sftp_credentials,  # type: ignore[call-arg]
        )

        with ssh.open_sftp() as sftp_client:
            sftp_client.chdir("output")

            # List all files in the directory
            files = sftp_client.listdir()

            # Filter files that end with .txt and cast to Path
            txt_files = [Path(f) for f in files if f.endswith(".txt")]

            awaiting_downloads.extend(txt_files)

    return awaiting_downloads


def get_match(
    pending_downloads: List[Tuple[Path, Path, Path]],
    awaiting_downloads: List[Path],
) -> List[Tuple[Path, Path, Path]]:
    """
    Match the pending downloads with the files awaiting download.
    This is done by checking if the transcript name in the SFTP upload path
    is present in the name of the files awaiting download.

    Args:
        pending_downloads (List[Tuple[Path, Path, Path]]): List of tuples
            containing the transcription source path, SFTP upload path,
            and transcription destination path.
        awaiting_downloads (List[Path]): List of remote paths to the files
            awaiting download.

    Returns:
        List[Tuple[Path, Path, Path]]: List of tuples containing the
            transcription source path, SFTP upload path, and
            transcription destination path for the matched files.
    """
    matches: List[Tuple[Path, Path, Path]] = []

    for (
        transcription_source_path,
        sftp_upload_path,
        transcription_destination_path,
    ) in pending_downloads:
        transcript_name = sftp_upload_path.stem
        for awaiting_download in awaiting_downloads:
            if transcript_name in awaiting_download.name:
                matches.append(
                    (
                        transcription_source_path,
                        sftp_upload_path,
                        transcription_destination_path,
                    )
                )

    return matches


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

    logger.info("Getting pending downloads from database...")
    pending_downloads = get_pending_downloads_local(config_file=config_file)

    if not pending_downloads:
        logger.info("No pending downloads found.")
        sys.exit(0)
    else:
        logger.info(f"Found {len(pending_downloads)} pending downloads.")

    logger.info("Getting files awaiting download from SFTP...")
    awaiting_downloads = get_files_awaiting_download(config_file=config_file)
    if not awaiting_downloads:
        logger.info("No files awaiting download found.")
        sys.exit(0)
    else:
        logger.info(f"Found {len(awaiting_downloads)} files awaiting download.")

    logger.info("Matching pending downloads with awaiting downloads...")
    matches = get_match(pending_downloads, awaiting_downloads)
    if not matches:
        logger.info("No matches found.")
        sys.exit(0)
    else:
        logger.info(f"Found {len(matches)} matches.")

    with utils.get_progress_bar() as progress:
        task = progress.add_task(
            description="Downloading matched files from SFTP...", total=len(matches)
        )
        for (
            transcription_source_path,
            sftp_upload_path,
            transcription_destination_path,
        ) in matches:
            transcript_file_name = (  # pylint: disable=invalid-name
                f"{sftp_upload_path.stem}.txt"
            )
            transcribeme_output_root = Path("/output")
            transcript_path = transcribeme_output_root / transcript_file_name

            study_name = transcript_file_name.split("_", maxsplit=1)[0]
            if "audioJournal" in transcript_file_name:
                archive_directory = (  # pylint: disable=invalid-name
                    f"{study_name}_journals_archive"
                )
            else:
                archive_directory = (  # pylint: disable=invalid-name
                    f"{study_name}_archive"
                )
            transcript_archive_path = (
                transcribeme_output_root / archive_directory / transcript_file_name
            )

            sftp_credentials = sftp.get_sftp_credentials(
                config_file=config_file, sftp_credentials_name="transcribeme_sftp"
            )
            # sftp_hostname = sftp_credentials["hostname"]
            # sftp_username = sftp_credentials["username"]
            # sftp_password = sftp_credentials["password"]
            # sftp_port = int(sftp_credentials["port"])

            # logger.debug(f"Connecting to SFTP server {sftp_hostname}...")

            # Exponential backoff for SSH connection
            # Retry connecting to the SFTP server if the connection fails
            connected: bool = False
            retry_timeout: int = 4

            while connected is False:
                try:
                    with paramiko.SSHClient() as ssh:
                        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                        ssh.connect(
                            # hostname=sftp_hostname,
                            # username=sftp_username,
                            # password=sftp_password,
                            # port=sftp_port,
                            look_for_keys=False,
                            timeout=120,
                            **sftp_credentials,  # type: ignore[call-arg]
                        )
                        connected = True

                        with Timer() as timer:
                            with ssh.open_sftp() as sftp_client:
                                logger.info(
                                    f"Downloading [remote] {transcript_path} to"
                                    f"[local] {transcription_destination_path}"
                                )
                                sftp.sftp_download_file(
                                    sftp_client,
                                    transcript_path,
                                    transcription_destination_path,
                                )
                                logger.info(
                                    f"[remote] Moving {transcript_path} to {transcript_archive_path}"
                                )
                                sftp.sftp_move_file(
                                    sftp_client,
                                    transcript_path,
                                    transcript_archive_path,
                                )
                                logger.info(f"[remote] Deleting {sftp_upload_path}")
                                sftp.sftp_delete_file(sftp_client, sftp_upload_path)
                except paramiko.SSHException as e:
                    logger.error(f"SSH connection error: {e}")
                    logger.info(f"Retrying in {retry_timeout} seconds...")

                    time.sleep(retry_timeout)

                    retry_timeout *= 2

                    if retry_timeout > 60:
                        logger.error("Max retry timeout reached. Exiting.")
                        sys.exit(1)
                    continue

            # Move transcription_source_path from 'pending_audio' to 'completed'
            completed_path = (
                transcription_source_path.parent.parent
                / "completed_audio"
                / transcription_source_path.name
            )
            logger.info(f"Moving {transcription_source_path} to {completed_path}")
            completed_path.parent.mkdir(parents=True, exist_ok=True)
            transcription_source_path.rename(completed_path)

            pull = TranscribemePull(
                transcription_destination_path=transcription_destination_path,
                sftp_download_path=transcription_source_path,
                sftp_archive_path=transcript_archive_path,
                sftp_download_timestamp=datetime.now(),
                sftp_download_duration_s=timer.duration,  # type: ignore[union-attr]
                completed_audio_file_path=completed_path,
            )
            insert_query = pull.to_sql()  # pylint: disable=invalid-name
            db.execute_queries(
                config_file=config_file,
                queries=[insert_query],
            )
            progress.update(task, advance=1)

    logger.info("Done")
    console.rule("[bold red]End of script")
