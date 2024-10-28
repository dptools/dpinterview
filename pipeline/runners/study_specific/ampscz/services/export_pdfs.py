#!/usr/bin/env python
"""
Export PDFs for the AMP-SCZ study.

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
from typing import Tuple, Literal, Optional
from datetime import datetime

from rich.logging import RichHandler

from pipeline import orchestrator
from pipeline.helpers import cli, dpdash, utils, db
from pipeline.models.exported_assets import ExportedAsset

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


def get_file_to_export(config_file: Path) -> Optional[Tuple[Path, str]]:
    """
    Get the file to export, that has not been exported yet.
    """
    query = """
    SELECT pr_path, interview_name
    FROM pdf_reports
    WHERE pr_version LIKE '%%anon%%'
        AND pr_path NOT IN (
            SELECT asset_path
            FROM exported_assets
        ) AND interview_name IN (
            SELECT interview_name
            FROM exported_assets
        )
    ORDER BY RANDOM()
    LIMIT 1;
    """

    df = db.execute_sql(
        query=query,
        config_file=config_file,
    )

    if not df.empty:
        file_to_export = Path(df["pr_path"].values[0])
        interview_name = df["interview_name"].values[0]

        return file_to_export, interview_name

    return None


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
            relative_path = path_parts[processed_idx + 1:]
            destination_path = export_path / Path(*relative_path)

    return destination_path


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

    while True:
        params = get_file_to_export(config_file=config_file)

        if params is None:
            logger.info("No more files to export.")
            break

        file_to_export, interview_name = params

        logger.info(f"Exporting file: {file_to_export}")

        export_params: Tuple[
            Path, Literal["file", "directory"], Literal["GENERAL", "PROTECTED"], str
        ] = (
            file_to_export,
            "file",
            "GENERAL",
            "anonymized_pdf_report",
        )

        destination_path = get_export_path(
            interview_name=interview_name,
            exportable_asset=export_params,
            config_file=config_file,
        )

        asset_path, asset_type, asset_export_type, asset_tag = export_params
        asset: ExportedAsset = ExportedAsset(
            interview_name=interview_name,
            asset_path=asset_path,
            asset_type=asset_type,
            asset_export_type=asset_export_type,
            asset_tag=asset_tag,
            asset_destination=destination_path,
            aset_exported_timestamp=datetime.now(),
        )

        source_path = asset_path
        dest_path = destination_path

        if not dest_path.parent.exists():
            dest_path.parent.mkdir(parents=True)

        logger.info(f"Copying {source_path} -> {dest_path}")
        cli.copy(source_path, dest_path)

        query = asset.to_sql()
        db.execute_queries(config_file=config_file, queries=[query])

    logger.info("Exporting complete.")
