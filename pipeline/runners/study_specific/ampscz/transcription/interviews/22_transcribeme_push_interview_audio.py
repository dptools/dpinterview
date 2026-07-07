#!/usr/bin/env python
"""
Push Interview Audio to Transcribeme for transcription.
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
from pipeline.helpers import cli, db, sftp, utils, dpdash
from pipeline.helpers.timer import Timer
from pipeline.models.transcribeme.transcribeme_push import TranscribemePush

MODULE_NAME = "pipeline.runners.ampscz.transcribeme_push_interview_audio"

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
) -> Optional[Tuple[Path, str, str, str, str]]:
    """
    Get the next file to process from the database.

    This function queries the database for interview audio that have not been
    transcribed yet.

    Args:
        config_file (Path): Path to the config file.
        study_id (str): Study ID to filter the audio interviews.

    Returns:
        Optional[Tuple[Path, str, str, str, str]]: A tuple containing the interview path,
            interview name, interview type, subject ID, and study ID.
            Returns None if no files are found.
    """
    query = f"""
    SELECT
        transcribeme.audio_qc.aqc_source_path AS wav_path,
        public.interviews.interview_name,
        public.interviews.interview_type,
        public.interviews.subject_id ,
        public.interviews.study_id
    FROM transcribeme.wav_conversion
    LEFT JOIN public.interview_files ON wav_conversion.wc_source_path = public.interview_files.interview_file
    LEFT JOIN public.interview_parts USING (interview_path)
    LEFT JOIN public.interviews USING (interview_name)
    LEFT JOIN transcribeme.audio_qc ON
        transcribeme.wav_conversion.wc_destination_path = transcribeme.audio_qc.aqc_source_path
    WHERE public.interview_files.interview_file IS NOT NULL AND
        transcribeme.audio_qc.aqc_passed IS TRUE AND
        transcribeme.audio_qc.aqc_source_path NOT IN (
            SELECT transcription_source_path
            FROM transcribeme.transcribeme_push
        ) AND public.interviews.study_id = '{study_id}'
    ORDER BY public.interviews.interview_name
    LIMIT 1
    """

    result_df = db.execute_sql(
        config_file=config_file,
        query=query,
    )

    if result_df.empty:
        return None

    wav_path = Path(result_df.iloc[0]["wav_path"])
    interview_name = result_df.iloc[0]["interview_name"]
    interview_type = result_df.iloc[0]["interview_type"]
    subject_id = result_df.iloc[0]["subject_id"]
    study_id = result_df.iloc[0]["study_id"]

    return wav_path, interview_name, interview_type, subject_id, study_id


def get_interview_session_number(
    interview_name: str, interview_type: str, subject_id: str, config_file: Path
) -> Optional[int]:
    """
    Get the interview session number for the given interview, ordered by interview date / day.

    Args:
        interview_name (str): Name of the interview.
        interview_type (str): Type of the interview.
        subject_id (str): Subject ID.

    Returns:
        Optional[int]: The session number if found, otherwise None.
    """
    query = f"""
    SELECT
        interview_name
    FROM public.interviews
    WHERE
        interview_type = '{interview_type}' AND
        subject_id = '{subject_id}'
    ORDER BY interview_name
    """

    result_df = db.execute_sql(
        config_file=config_file,
        query=query,
    )

    if result_df.empty:
        return None

    # Get the interview session number based on the position of the interview name
    try:
        session_number = result_df["interview_name"].tolist().index(interview_name) + 1
        return session_number
    except ValueError:
        logger.warning(
            f"Interview {interview_name} (subject {subject_id}, type "
            f"{interview_type}) not found among its own study's interviews "
            f"list; cannot compute a session number."
        )
        return None


def get_subject_interviews_root(
    config_file: Path, subject_id: str, study_id: str
) -> Path:
    """
    Get the root directory for the subject's interviews.

    Args:
        config_file (Path): Path to the config file.
        subject_id (str): Subject ID.
        study_id (str): Study ID.

    Returns:
        Path: Path to the subject's interviews root directory.
    """
    data_root = orchestrator.get_data_root(config_file=config_file, enforce_real=True)
    subject_root = data_root / "PROTECTED" / study_id / "processed" / subject_id
    subject_interviews_root = subject_root / "interviews"

    return subject_interviews_root


def construct_transcript_destination_path(
    subject_interviews_root: Path,
    interview_name: str,
    interview_type: str,
    interview_session_number: int,
) -> Path:
    """
    Construct the path for the transcript destination file.

    This is where the transcript will be saved after transcription.

    Args:
        subject_interviews_root (Path): Path to the subject's interviews root directory.
        interview_name (str): Name of the interview.
        interview_type (str): Type of the interview.
        interview_session_number (int): Session number of the interview.

    Returns:
        Path: Path to the transcript destination file.
    """
    transcription_destination_directory = (
        subject_interviews_root / interview_type / "transcripts"
    )
    if not transcription_destination_directory.exists():
        transcription_destination_directory.mkdir(parents=True, exist_ok=True)

    dpdash_dict = dpdash.parse_dpdash_name(interview_name)

    dpdash_dict["data_type"] = "interviewAudioTranscript"
    dpdash_dict["category"] = interview_type

    transcript_name = dpdash.get_dpdash_name_from_dict(dpdash_dict)
    transcript_name = transcript_name.replace("-", "_")
    full_transcript_name = (
        f"{transcript_name}_session{interview_session_number:03d}.txt"
    )

    transcription_destination_path = (
        transcription_destination_directory / full_transcript_name
    )

    return transcription_destination_path


def construct_sftp_upload_path(
    interview_name: str, interview_type: str, source_language: str
) -> str:
    """
    Returns the SFTP upload path for the audio file.

    Args:
        interview_name (str): Name of the interview.
        interview_type (str): Type of the interview.
        source_language (str): Source language of the audio file.

    Returns:
        str: SFTP upload path for the audio file.
    """
    dpdash_dict = dpdash.parse_dpdash_name(interview_name)
    dpdash_dict["data_type"] = "interviewAudioTranscript"
    dpdash_dict["category"] = interview_type

    dpdash_name = dpdash.get_dpdash_name_from_dict(dpdash_dict)

    # Append source language to the file name, before the file extension
    file_name = f"{dpdash_name}-{source_language.lower()}.wav"

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
        audio_path, interview_name, interview_type, subject_id, study_id = (
            file_to_process
        )
        source_language = study_language_map.get(study_id, "ENGLISH")
        logger.info(
            f"Handling audio: {audio_path} [{source_language}]",
            extra={"markup": True},
        )

        subject_interviews_root = get_subject_interviews_root(
            config_file=config_file, subject_id=subject_id, study_id=study_id
        )
        interview_session_number = get_interview_session_number(
            interview_name=interview_name,
            interview_type=interview_type,
            subject_id=subject_id,
            config_file=config_file,
        )
        if interview_session_number is None:
            logger.warning(
                f"Could not determine session number for interview: {interview_name}"
            )
            interview_session_number = 0

        # Upload to Transcribeme
        logger.info(
            f"[green]Uploading {audio_path} to Transcribeme SFTP server...",
            extra={"markup": True},
        )
        transcription_destination_path = construct_transcript_destination_path(
            subject_interviews_root=subject_interviews_root,
            interview_name=interview_name,
            interview_type=interview_type,
            interview_session_number=interview_session_number,
        )
        sftp_upload_path = construct_sftp_upload_path(
            interview_name=interview_name,
            interview_type=interview_type,
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
                    logger.info(f"Uploading {audio_path} to {sftp_upload_path}...")
                    sftp_client.put(audio_path, sftp_upload_path)
                    logger.info(f"File {sftp_upload_path} uploaded successfully.")

        push_obj = TranscribemePush(
            transcription_source_path=audio_path,
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
            f"Inserted {audio_path} to database.",
            extra={"markup": True},
        )
