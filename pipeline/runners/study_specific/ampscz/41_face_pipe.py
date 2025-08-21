#!/usr/bin/env python
"""
Run Face Pipe on interviews
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
import tempfile

import pandas as pd
from rich.logging import RichHandler

from pipeline import orchestrator
from pipeline.helpers import cli, db, utils
from pipeline.helpers.timer import Timer
from pipeline.models.facepipe.facepipe_qc import FacepipeQc
from pipeline.models.facepipe.facepipe_run import FacepipeRun

MODULE_NAME = "pipeline.runners.ampscz.face_pipe"

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

PYTHON_PATH = Path("/opt/software/env/face-analysis/bin/python")
FEATURE_EXTRACTION_SCRIPT_PATH = Path(
    "/opt/software/face-pipe/hybrid/hybrid_video_pipeline_server.py"
)
QC_SCRIPT_PATH = Path("/opt/software/face-pipe/hybrid/video_stats_qc_only.py")


def get_file_to_process(config_file: Path) -> Optional[pd.DataFrame]:
    """
    Get the file to process from the config file.

    Args:
        config_file (Path): Path to the config file.

    Returns:
        pd.DataFrame: DataFrame containing the file to process.
    """
    sql_query = """
    WITH passed_qc AS (
        SELECT
            mq.qc_target_id,
            mq.qc_data ->> 'comments' AS qc_comments
        FROM manual_qc AS mq
        WHERE mq.qc_target_type = 'interview'
        AND mq.qc_data ->> 'hasNoIssues' = 'true'
    ),
    random_eligible_interview AS (
        SELECT DISTINCT ip.interview_name
        FROM video_streams AS vs
        LEFT JOIN video_quick_qc AS vq USING(video_path)
        LEFT JOIN decrypted_files AS df ON vq.video_path = df.destination_path
        LEFT JOIN interview_files AS iff ON df.source_path = iff.interview_file
        LEFT JOIN interview_parts AS ip USING(interview_path)
        LEFT JOIN passed_qc AS pq ON ip.interview_name = pq.qc_target_id
        WHERE vq.has_black_bars AND
            pq.qc_target_id IS NOT NULL AND
            vs.video_path NOT IN (
                SELECT fp_source_video_path
                FROM facepipe.facepipe_runs
            ) AND
            ip.is_primary
        LIMIT 1
    )
    SELECT
        i.*,
        -- Conditionally select the path: exported_assets.asset_destination if available, else vs.vs_path
        CASE
            WHEN ea.asset_destination IS NOT NULL THEN ea.asset_destination
            ELSE vs.vs_path
        END AS parsed_vs_path,
        vs_path,
        ir_role,
        vs.video_path
    FROM video_streams AS vs
    LEFT JOIN video_quick_qc AS vq USING(video_path)
    LEFT JOIN decrypted_files AS df ON vq.video_path = df.destination_path
    LEFT JOIN interview_files AS iff ON df.source_path = iff.interview_file
    LEFT JOIN interview_parts AS ip USING(interview_path)
    LEFT JOIN interviews AS i USING (interview_name)
    LEFT JOIN passed_qc AS pq ON ip.interview_name = pq.qc_target_id
    LEFT JOIN public.exported_assets AS ea
        ON vs.vs_path = ea.asset_path AND ea.asset_tag = 'streams'
    WHERE
        ip.interview_name IN (SELECT interview_name FROM random_eligible_interview);
    """

    db_df = db.execute_sql(
        config_file=config_file,
        query=sql_query,
    )

    if db_df.empty:
        logger.info("No interviews found to process.")
        return None

    return db_df


def move_assets_to_phoenix(
    streams_df: pd.DataFrame,
    assets_dir: Path,
    config_file: Path,
) -> Tuple[Path, Dict]:
    """
    Moves the generated assets to the Phoenix directory.

    Args:
        streams_df (pd.DataFrame): DataFrame containing the streams to process.
        assets_dir (Path): Path to the directory where assets will be moved.

    Returns:
        Path: Path to the extracted featuess (.csv file).
        Dict: Metadata dictionary containing the metadata for this face pipe run.
    """

    interview_name: str = streams_df["interview_name"].unique().tolist()[0]
    interview_type = streams_df["interview_type"].unique().tolist()[0]
    subject_id: str = streams_df["subject_id"].unique().tolist()[0]
    study_id: str = streams_df["study_id"].unique().tolist()[0]

    data_root = orchestrator.get_data_root(
        config_file=config_file,
        enforce_real=True,
    )

    export_root: Path = (
        data_root
        / "GENERAL"
        / study_id
        / "processed"
        / subject_id
        / "interviews"
        / interview_type
        / "face_pipe"
        / interview_name
    )
    export_root.mkdir(parents=True, exist_ok=True)

    assets_dir = Path(assets_dir).resolve()
    assets_generated = list(assets_dir.glob("**/*.csv"))

    interview_video_run_time_files = [
        f for f in assets_generated if "InterviewVideoRunTime" in f.name
    ]
    feature_files = [f for f in assets_generated if "InterviewVideoFeatures" in f.name]

    if len(interview_video_run_time_files) != 1:
        logger.error(
            f"Expected 1 InterviewVideoRunTime file, \
found {len(interview_video_run_time_files)}. Skipping..."
        )
        raise ValueError(
            f"Expected 1 InterviewVideoRunTime file, \
found {len(interview_video_run_time_files)}."
        )

    if len(feature_files) != 1:
        logger.error(
            f"Expected 1 InterviewVideoFeatures file, found {len(feature_files)}. Skipping..."
        )
        raise ValueError(
            f"Expected 1 InterviewVideoFeatures file, found {len(feature_files)}."
        )

    metadata_df = pd.read_csv(interview_video_run_time_files[0])
    metadata_df.drop(columns=["ID"], inplace=True)
    metadata_dict = metadata_df.set_index("role").to_dict("index")

    feature_file = feature_files[0]

    new_feature_file = export_root / feature_file.name
    logger.info(f"Moving feature file {feature_file} to {new_feature_file}")
    cli.copy(feature_file, new_feature_file)

    return new_feature_file, metadata_dict


def process_interview(streams_df: pd.DataFrame, config_file: Path) -> None:
    """
    Process the interview with Face Pipe.

    Args:
        streams_df (pd.DataFrame): DataFrame containing the streams to process.
        config_file (Path): Path to the config file.

    Returns:
        None
    """
    interview_name: str = streams_df["interview_name"].unique().tolist()[0]
    video_path: Path = Path(streams_df["video_path"].unique().tolist()[0])

    logger.info(f"Processing interview: {interview_name} with video path: {video_path}")

    streams = streams_df["parsed_vs_path"].unique().tolist()
    streams = [Path(s).resolve() for s in streams]

    for stream in streams:
        stream = Path(stream).resolve()
        if not stream.exists():
            logger.error(f"Stream {stream} does not exist. Skipping...")
            return

    with tempfile.TemporaryDirectory(
        prefix="face_pipe", suffix=f"_{interview_name}",
    ) as temp_dir:
        temp_dir = Path(temp_dir).resolve()
        # temp_dir = Path("/home/dm2637/temp").resolve() / f"face_pipe_{interview_name}"
        # temp_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"Using temporary directory: {temp_dir}")

        streams_dir = temp_dir / "streams"
        out_dir = temp_dir / "out"
        log_dir = temp_dir / "logs"

        streams_dir.mkdir(parents=True, exist_ok=True)
        out_dir.mkdir(parents=True, exist_ok=True)
        log_dir.mkdir(parents=True, exist_ok=True)

        for stream in streams:
            cli.copy(stream, streams_dir)

        logger.info("Starting Face Pipe feature extraction...")

        with Timer() as timer:
            command_array: List[str | Path] = [
                PYTHON_PATH,
                FEATURE_EXTRACTION_SCRIPT_PATH,
                streams_dir,
                out_dir,
                log_dir,
            ]
            cli.execute_commands(
                command_array=command_array,
                shell=True,
            )
            logger.info("Face Pipe feature extraction completed.")

            logger.info("Parsing Logs for QC measures...")
            qc_command_array: List[str | Path] = [
                PYTHON_PATH,
                QC_SCRIPT_PATH,
                out_dir,
                log_dir,
                streams_dir,
            ]
            cli.execute_commands(
                command_array=qc_command_array,
                shell=True,
            )

        elapsed_time = timer.duration

        facepipe_qc = FacepipeQc(
            fp_source_video_path=video_path,
            interview_name=interview_name,
            qc_file_path=streams_dir / "all_video_stats.csv",
        )

        feature_csv, metadata_dict = move_assets_to_phoenix(
            streams_df=streams_df,
            assets_dir=out_dir,
            config_file=config_file,
        )

        facepipe_run = FacepipeRun(
            fp_source_video_path=video_path,
            fp_features_csv_path=feature_csv,
            fp_run_metadata=metadata_dict,
            fp_duration_s=elapsed_time,  # type: ignore
            fp_timestamp=datetime.now(),
        )

        # Log the FacepipeQc and FacepipeRun to the database
        sql_queries = [
            facepipe_run.to_sql(),
            facepipe_qc.to_sql(),
        ]

        db.execute_queries(
            config_file=config_file,
            queries=sql_queries,
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog=MODULE_NAME, description="Run Face Pipe on QC'd interviews."
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

    logger.info("Starting face_pipe loop...", extra={"markup": True})
    orchestrator.redirect_temp_dir(config_file=config_file)

    while True:
        file_to_process_df = get_file_to_process(config_file=config_file)

        if file_to_process_df is None:
            if COUNTER > 0:
                orchestrator.log(
                    config_file=config_file,
                    module_name=MODULE_NAME,
                    message=f"Processed {COUNTER} interviews with Face Pipe.",
                )
                COUNTER = 0

            # Snooze if no files to process
            orchestrator.snooze(config_file=config_file)
            continue

        process_interview(
            streams_df=file_to_process_df,
            config_file=config_file,
        )
