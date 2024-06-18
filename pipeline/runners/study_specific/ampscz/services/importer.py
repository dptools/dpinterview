#!/usr/bin/env python
"""
Copies the files to the destination directory, in lieu of decryption.

Assumptions:
- The files are already decrypted / not encrypted.

Note: This script should have read access to the PHOENIX directory.
"""

import sys
from pathlib import Path

file = Path(__file__).resolve()
parent = file.parent
ROOT = None
for parent in file.parents:
    if parent.name == "av-pipeline-v2":
        ROOT = parent
sys.path.append(str(ROOT))

# remove current directory from path
try:
    sys.path.remove(str(parent))
except ValueError:
    pass

import argparse
import logging
import shutil

from rich.logging import RichHandler

from pipeline import orchestrator
from pipeline.helpers import cli, utils, dpdash
from pipeline.helpers.timer import Timer
from pipeline.models.decrypted_files import DecryptedFile
from pipeline.models.interview_files import InterviewFile
from pipeline.models.interviews import Interview

MODULE_NAME = "ampscz-importer"
INSTANCE_NAME = MODULE_NAME

logger = logging.getLogger(__name__)
logger = logging.getLogger(MODULE_NAME)
logargs = {
    "level": logging.DEBUG,
    # "format": "%(asctime)s - %(process)d - %(name)s - %(levelname)s - %(message)s",
    "format": "%(message)s",
    "handlers": [RichHandler(rich_tracebacks=True)],
}
logging.basicConfig(**logargs)

console = utils.get_console()


def import_file(source_path: Path, destination_path: Path) -> None:
    """
    Imports the file to the destination directory.

    Args:
        source_path (Path): The path to the file to import.
        destination_path (Path): The path to the destination directory.
    """
    logger.info(f"Importing file: {source_path} -> {destination_path}")

    with utils.get_progress_bar() as progress:
        progress.add_task("Copying file...", total=None)

        if not destination_path.parent.exists():
            destination_path.parent.mkdir(parents=True)

        shutil.copy(source_path, destination_path)


def get_external_audio_source(config_file: Path) -> Path:
    """
    Get external sources from the config file.
    """
    config_params = utils.config(config_file, section="external")
    if "audio_processing_pipeline_source" not in config_params:
        raise KeyError("audio_processing_pipeline_source not found in config file.")

    return Path(config_params["audio_processing_pipeline_source"])


def generate_dest_audio_file_name(
    interview_name: str,
    audio_file: Path,
) -> str:
    """
    Generates a destination file name for the audio file.

    Args:
        interview_name (str): The name of the interview.

    Returns:
        str: The destination file name.
    """
    file_name = audio_file.stem

    dpdash_dict = dpdash.parse_dpdash_name(interview_name)
    dpdash_dict["category"] = "audio"
    dpdash_dict["optional_tags"] = [file_name]

    dest_file_name = dpdash.get_dpdash_name_from_dict(dpdash_dict)

    return dest_file_name


def import_audio_files(config_file: Path, interview_name: str, dest_root: Path) -> None:
    """
    Imports all audio files for the given interview.

    Args:
        config_file (Path): The path to the configuration file.
        interview_name (str): The name of the interview.
        dest_root (Path): The path to the destination directory.
    """
    audio_files = InterviewFile.get_interview_files_with_tag(
        config_file=config_file, interview_name=interview_name, tag="diarized"
    )

    dest_dir = dest_root / interview_name

    for audio_file in audio_files:
        dest_file_name = generate_dest_audio_file_name(
            interview_name=interview_name, audio_file=audio_file
        )
        file_destination = dest_dir / f"{dest_file_name}{audio_file.suffix}"

        import_file(source_path=audio_file, destination_path=file_destination)

        orchestrator.fix_permissions(config_file=config_file, file_path=dest_root)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="importer", description="Import files from the shared directory."
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
    data_root = orchestrator.get_data_root(config_file=config_file)

    external_audio_source = get_external_audio_source(config_file=config_file)

    COUNTER = 0

    while True:
        files_to_decrypt = DecryptedFile.get_files_pending_decrytion(
            config_file=config_file
        )

        if files_to_decrypt.empty:
            logger.info("No files to import.")
            orchestrator.snooze(config_file=config_file)

        for index, row in files_to_decrypt.iterrows():
            source_path = Path(row["source_path"])
            destination_path = Path(row["destination_path"])

            if not source_path.exists():
                logger.error(f"Error: File to import does not exist: {source_path}")
                sys.exit(1)

            if destination_path.exists():
                logger.warning(
                    f"Destination file already exists. Removing: {destination_path}"
                )
                destination_path.unlink()

            interview_name = Interview.get_interview_name(
                config_file=config_file, interview_file=source_path
            )

            with Timer() as timer:
                import_file(source_path=source_path, destination_path=destination_path)

                if interview_name is not None:
                    import_audio_files(
                        config_file=config_file,
                        interview_name=interview_name,
                        dest_root=external_audio_source,
                    )
                else:
                    logger.warning(
                        f"No interview found for file: {source_path}. Skipping..."
                    )

                orchestrator.fix_permissions(
                    config_file=config_file, file_path=data_root
                )

            DecryptedFile.update_decrypted_status(
                config_file=config_file,
                file_path=source_path,
                process_time=timer.duration,
            )
            COUNTER += 1

        if COUNTER >= 10:
            orchestrator.log(
                config_file=config_file,
                module_name=MODULE_NAME,
                message=f"Imported {COUNTER} files.",
            )
            COUNTER = 0
