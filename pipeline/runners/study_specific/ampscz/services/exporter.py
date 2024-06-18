#!/usr/bin/env python
"""
Moves the assets generateds by various services to the destination directory.

Note: This script should have write access to the PHOENIX directory.
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
from typing import List, Tuple, Literal, Optional

from rich.logging import RichHandler
import pandas as pd

from pipeline import orchestrator
from pipeline.helpers import cli, utils, dpdash, db
from pipeline.models.interview_roles import InterviewRole
from pipeline.models.exported_assets import ExportedAsset
from pipeline.models.faceprocessing_pipleine_metrics import (
    FaceProcessingPipelineMetrics,
)

MODULE_NAME = "ampscz-exporter"

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


def reconstruct_interview_name(face_processing_pipeline_directory_name: str) -> str:
    """
    Constructs the interview name from the face processing pipeline directory name.

    Args:
        face_processing_pipeline_directory_name (str): The face processing pipeline directory name.
    Returns:
        str: The interview name.
    """
    parts = face_processing_pipeline_directory_name.split("_")
    study_id = parts[0]
    subject_id = parts[1]
    # category = parts[2]
    interview_type = parts[3]
    day = parts[4]

    interview_name = f"{study_id}-{subject_id}-{interview_type}Interview-{day}"
    return interview_name


def get_faceprocessing_pipeline_exports(
    interview_output_dir: Path,
) -> List[
    Tuple[Path, Literal["file", "directory"], Literal["GENERAL", "PROTECTED"], str]
]:
    """
    Returns a list of tuples with the following structure:
    (Path to the file/directory, file/diretory (type), GENERAL or PROTECTED)

    Parameters
        - interview_output_dir (Path): Path to the directory containing the face
            processing pipeline outputs
    Returns
        - List of tuples with the structure (Path to the file/directory,
            file/diretory (type), GENERAL or PROTECTED)
    """
    required_exports = list(interview_output_dir.glob("*InterviewVideoFeatures*.csv"))

    exports = []
    for export in required_exports:
        exports.append((export, "file", "GENERAL", "face_processing_pipeline"))
    return exports


def duration_to_seconds(duration_str: str) -> int:
    """
    Converts a duration string in the format `H:M:S` to seconds.

    Parameters
        - duration_str (str): Duration string in the format `H:M:S`

    Returns
        - int: Duration in seconds
    """
    h, m, s = map(int, duration_str.split(":"))
    total_seconds = h * 3600 + m * 60 + s
    return total_seconds


def get_faceprocessing_pipeline_metrics(
    interview_name: str, interview_output_dir: Path
) -> List[FaceProcessingPipelineMetrics]:
    """
    Returns a list of FaceProcessingPipelineMetrics objects for the given interview.

    Parameters
        - interview_output_dir (Path): Path to the directory containing the face
            processing pipeline outputs

    Returns
        - List of FaceProcessingPipelineMetrics objects
    """
    metrics_path = list(interview_output_dir.glob("*InterviewVideoRunTime*.csv"))
    if not metrics_path:
        return []

    metrics_path = metrics_path[0]
    metrics_df = pd.read_csv(metrics_path)

    metrics: List[FaceProcessingPipelineMetrics] = []

    for _, row in metrics_df.iterrows():
        ir_role = InterviewRole(row["role"])
        fpm_landmark_time: str = row["landmark_time"]
        fpm_fpose_time: str = row["fpose_time"]
        fpm_au_time: str = row["au_time"]
        fpm_execution_time: str = row["execution_time"]
        fpm_record_time: str = row["record_time"]

        # convert from H:M:S to seconds
        fpm_landmark_time_s = duration_to_seconds(fpm_landmark_time)
        fpm_fpose_time_s = duration_to_seconds(fpm_fpose_time)
        fpm_au_time_s = duration_to_seconds(fpm_au_time)
        fpm_execution_time_s = duration_to_seconds(fpm_execution_time)
        fpm_record_time_s = duration_to_seconds(fpm_record_time)

        metric = FaceProcessingPipelineMetrics(
            interview_name=interview_name,
            ir_role=ir_role,
            fpm_landmark_time_s=fpm_landmark_time_s,
            fpm_fpose_time_s=fpm_fpose_time_s,
            fpm_au_time_s=fpm_au_time_s,
            fpm_execution_time_s=fpm_execution_time_s,
            fpm_record_time_s=fpm_record_time_s,
        )
        metrics.append(metric)

    return metrics


def get_pipeline_streams(interview_name: str, config_file: Path) -> List[Path]:
    """
    Returns the paths to the video streams generated by the pipeline for the given interview.

    Parameters
        - interview_name (str): Name of the interview
        - config_file (Path): Path to the config file
    Returns
        - List of paths to the video streams
    """
    query = f"""
SELECT vs_path FROM video_streams
INNER JOIN video_quick_qc USING(video_path)
INNER JOIN decrypted_files ON video_streams.video_path = decrypted_files.destination_path
INNER JOIN interview_files ON decrypted_files.source_path = interview_files.interview_file
INNER JOIN interviews USING (interview_path)
INNER JOIN subjects USING(subject_id, study_id)
WHERE interview_name = '{interview_name}'
"""

    df = db.execute_sql(config_file=config_file, query=query)

    streams = df["vs_path"].tolist()
    streams_path = [Path(stream) for stream in streams]

    return streams_path


def get_pipeline_frames(interview_name: str, config_file: Path) -> Optional[Path]:
    """
    Returns the path to the frames directory generated by the pipeline for the given interview.

    Parameters
        - interview_name: Name of the interview
        - config_file: Path to the config file

    Returns
        - Optional[Path]: Path to the frames directory
    """
    query = f"""
SELECT video_path FROM video_streams
INNER JOIN video_quick_qc USING(video_path)
INNER JOIN decrypted_files ON video_streams.video_path = decrypted_files.destination_path
INNER JOIN interview_files ON decrypted_files.source_path = interview_files.interview_file
INNER JOIN interviews USING (interview_path)
INNER JOIN subjects USING(subject_id, study_id)
WHERE interview_name = '{interview_name}'
"""

    video_path = db.fetch_record(config_file=config_file, query=query)

    if video_path:
        video_path = Path(video_path)
        frames_path = video_path.parent / "frames" / video_path.stem

        if frames_path.exists():
            return frames_path

    return None


def get_openface_assets(interview_name: str, config_file: Path) -> List[Path]:
    """
    Returns the paths to the OpenFace assets generated by the pipeline for the given interview.

    Parameters
        - interview_name (str): Name of the interview
        - config_file (Path): Path to the config file

    Returns
        - List of paths to the OpenFace assets (directories)
    """
    query = f"""
SELECT of_processed_path FROM openface
LEFT JOIN ffprobe_metadata ON fm_source_path = openface.vs_path
INNER JOIN video_streams USING (vs_path, video_path, ir_role)
INNER JOIN video_quick_qc USING(video_path)
INNER JOIN decrypted_files ON video_streams.video_path = decrypted_files.destination_path
INNER JOIN interview_files ON decrypted_files.source_path = interview_files.interview_file
INNER JOIN interviews USING (interview_path)
INNER JOIN subjects USING(subject_id, study_id)
WHERE interview_name = '{interview_name}'
"""

    df = db.execute_sql(config_file=config_file, query=query)

    assets = df["of_processed_path"].tolist()
    assets_path = [Path(asset) for asset in assets]

    return assets_path


def get_pipeline_exports(
    interview_name: str,
    config_file: Path,
) -> List[
    Tuple[Path, Literal["file", "directory"], Literal["GENERAL", "PROTECTED"], str]
]:
    """
    Returns a list of tuples with the following structure:
    (Path to the file/directory, file/diretory (type), GENERAL or PROTECTED)

    Parameters
        - interview_name: Name of the interview
        - config_file: Path to the config file

    Returns
        - List of tuples with the structure
            (Path to the file/directory, file/diretory (type), GENERAL or PROTECTED)
    """
    exports: List[
        Tuple[Path, Literal["file", "directory"], Literal["GENERAL", "PROTECTED"], str]
    ] = []

    streams = get_pipeline_streams(
        interview_name=interview_name, config_file=config_file
    )

    for stream in streams:
        exports.append((stream, "file", "PROTECTED", "streams"))

    frames = get_pipeline_frames(interview_name=interview_name, config_file=config_file)

    if frames:
        exports.append((frames, "directory", "PROTECTED", "frames"))

    openface_assets = get_openface_assets(
        interview_name=interview_name, config_file=config_file
    )

    for asset in openface_assets:
        exports.append((asset, "directory", "PROTECTED", "openface"))

    return exports


def construct_export_path(
    interview_name: str, export_type: Literal["GENERAL", "PROTECTED"], config_file: Path
) -> Path:
    """
    Constructs the export path for the given interview.

    Parameters
        - interview_name (str): Name of the interview
        - export_type (Literal["GENERAL", "PROTECTED"]): Type of export
        - config_file (Path): Path to the config file

    Returns
        - Path: Path to the export directory
    """
    data_root = orchestrator.get_data_root(config_file=config_file, enforce_real=True)

    dpdash_dict = dpdash.parse_dpdash_name(interview_name)
    study = dpdash_dict["study"]
    subject_id = dpdash_dict["subject"]
    interview_type = utils.camel_case_split(dpdash_dict["data_type"])[0]  # type: ignore

    export_path = (
        data_root
        / export_type
        / study  # type: ignore
        / "processed"
        / subject_id
        / "interviews"
        / interview_type  # type: ignore
    )

    return export_path


def get_export_path(
    interview_name: str,
    exportable_asset: Tuple[
        Path, Literal["file", "directory"], Literal["GENERAL", "PROTECTED"], str
    ],
    config_file: Path,
) -> Path:
    """
    Constructs the export path for the given asset.

    Parameters
        - interview_name (str): Name of the interview
        - exportable_asset
            (Tuple[Path, Literal["file", "directory"], Literal["GENERAL", "PROTECTED"], str]):
            Tuple containing the asset path, type, export type and tag
        - config_file (Path): Path to the config file

    Returns
        - Path: Path to the export directory
    """
    asset_path, _, asset_export_type, asset_tag = exportable_asset

    export_path = construct_export_path(
        interview_name=interview_name,
        export_type=asset_export_type,
        config_file=config_file,
    )

    match asset_tag:
        case "face_processing_pipeline":
            destination_path = export_path / asset_path.name
        case _:
            # remove fake data_root from asset_path
            path_parts = list(asset_path.parts)
            processed_idx = path_parts.index("processed")
            if "decrypted" in path_parts:
                processed_idx = path_parts.index("decrypted")
            relative_path = path_parts[processed_idx + 1 :]
            destination_path = export_path / Path(*relative_path)

    return destination_path


def is_interview_export_ready(interview_name: str, config_file: Path) -> bool:
    """
    Checks if the interview export is ready

    Parameters
        - interview_name: Name of the interview
        - config_file: Path to the config file

    Returns
        - True if the interview export is ready, False otherwise
    """

    # Reports are the final stage of the av pipeline
    # check if the report has been generated

    query = f"""
    SELECT * FROM pdf_reports
    WHERE interview_name = '{interview_name}'
    """

    df = db.execute_sql(config_file=config_file, query=query)

    if df.empty:
        return False

    return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="exporter", description="Export pipeline assets to the shared directory."
    )
    parser.add_argument(
        "-c", "--config", type=str, help="Path to the config file.", required=False
    )
    parser.add_argument(
        "-d",
        "--debug",
        type=bool,
        help="Enable debug mode.",
        default=False,
        required=False,
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

    debug: bool = args.debug

    utils.configure_logging(
        config_file=config_file, module_name=MODULE_NAME, logger=logger
    )

    console.rule(f"[bold red]{MODULE_NAME}")
    logger.info(f"Using config file: {config_file}")

    logger.info(f"Debug mode: {debug}")
    if debug:
        logger.warning("Debug mode enabled. No files will be copied.")

    external_config = utils.config(path=config_file, section="external")
    face_prcessing_output_dir = external_config["face_processing_pipeline_export"]
    face_prcessing_output_dir_path = Path(face_prcessing_output_dir)

    # list directories in face_prcessing_output_dir
    directories_to_export = list(face_prcessing_output_dir_path.glob("*"))

    for directory in directories_to_export:
        directory_name = directory.name
        interview_name = reconstruct_interview_name(directory_name)

        IS_EXPORT_READY = is_interview_export_ready(
            interview_name=interview_name, config_file=config_file
        )

        if not IS_EXPORT_READY:
            logger.warning(f"Skipping export for {interview_name}. Export not ready.")
            continue

        logger.info(f"Exporting assets for {interview_name}")

        exports: List[
            Tuple[
                Path, Literal["file", "directory"], Literal["GENERAL", "PROTECTED"], str
            ]
        ] = []

        pipeline_exports = get_pipeline_exports(
            interview_name=interview_name, config_file=config_file
        )
        exports.extend(pipeline_exports)

        faceprocessing_exports = get_faceprocessing_pipeline_exports(directory)
        exports.extend(faceprocessing_exports)

        queries: List[str] = []

        for exportable_asset in exports:
            asset_path, asset_type, asset_export_type, asset_tag = exportable_asset
            destination_path = get_export_path(
                interview_name=interview_name,
                exportable_asset=exportable_asset,
                config_file=config_file,
            )

            asset: ExportedAsset = ExportedAsset(
                interview_name=interview_name,
                asset_path=asset_path,
                asset_type=asset_type,
                asset_export_type=asset_export_type,
                asset_tag=asset_tag,
                asset_destination=destination_path,
            )

            if asset.asset_type == "file":
                logger.debug(f"Copying {asset.asset_path} -> {asset.asset_destination}")
                if not debug:
                    source = asset.asset_path
                    destination = asset.asset_destination
                    if not destination.parent.exists():
                        destination.parent.mkdir(
                            parents=True, exist_ok=True
                        )
                    shutil.copy2(source, destination)
            elif asset.asset_type == "directory":
                logger.debug(f"Copying {asset.asset_path} -> {asset.asset_destination}")
                if not debug:
                    shutil.copytree(asset.asset_path, asset.asset_destination)
            else:
                logger.error(f"Unknown asset type: {asset.asset_type} for {asset}")
                sys.exit(1)

            query = asset.to_sql()
            queries.append(query)

        metrics = get_faceprocessing_pipeline_metrics(
            interview_name=interview_name, interview_output_dir=directory
        )
        for metric in metrics:
            if debug:
                logger.debug(
                    f"Inserting {metric} into face_processing_pipeline_metrics"
                )
            query = metric.to_sql()
            queries.append(query)

        if not debug:
            db.execute_queries(
                config_file=config_file,
                queries=queries,
            )

            # Delete the face processing pipeline directory
            logger.debug(f"Deleting {directory}")
            shutil.rmtree(directory)

            # Clean up assets
            for exportable_asset in exports:
                asset_path, _, _, asset_tag = exportable_asset
                if asset_tag == "frames":
                    continue
                logger.debug(f"Deleting {asset_path}")
                if asset_path.is_file():
                    asset_path.unlink()
                elif asset_path.is_dir():
                    shutil.rmtree(asset_path)

    logger.info("Export complete.")
