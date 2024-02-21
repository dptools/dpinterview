#!/usr/bin/env python
"""
Export consolidated metrics from study-specific 72 to CSV and HTML
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
import time
from datetime import timedelta
from typing import Optional

import pandas as pd
from rich.logging import RichHandler

from pipeline.helpers import cli, db, utils
from pipeline import healer

MODULE_NAME = "pipeline.runners.73_consolidate_metrics"
logger = logging.getLogger(MODULE_NAME)
logargs = {
    "level": logging.DEBUG,
    # "format": "%(asctime)s - %(process)d - %(name)s - %(levelname)s - %(message)s",
    "format": "%(message)s",
    "handlers": [RichHandler(rich_tracebacks=True)],
}
logging.basicConfig(**logargs)

console = utils.get_console()


def fetch_metrics_df(
    config_file: Path,
    study_id: str,
) -> pd.DataFrame:
    """
    Get metrics for a study from the database.

    Args:
        config_file (Path): Path to the configuration file.
        study_id (str): The study ID.

    Returns:
        pd.DataFrame: The metrics for the study.
    """

    query = f"""
    SELECT metrics FROM metrics
    INNER JOIN interviews USING (interview_name)
    WHERE study_id = '{study_id}'
    """

    metrics_df = db.execute_sql(config_file=config_file, query=query)

    # Normalize the metrics column to separate the metrics into their own columns
    metrics_df: pd.DataFrame = pd.concat(
        [
            metrics_df.drop(["metrics"], axis=1),
            pd.json_normalize(metrics_df["metrics"]),  # type: ignore
        ],
        axis=1,
    )

    time_cols = [
        "openface_metrics.start_time",
        "openface_metrics.end_time",
        "openface_metrics.duration",
    ]

    def timedelta_to_string(timedelta: timedelta) -> Optional[str]:
        """
        Convert a timedelta to a string in the format HH:MM:SS.

        Args:
            timedelta (timedelta): The timedelta to convert.

        Returns:
            str: The string representation of the timedelta.
        """
        if pd.isna(timedelta):
            return None
        return time.strftime("%H:%M:%S", time.gmtime(timedelta.total_seconds()))

    # Process the time columns
    for col in time_cols:
        metrics_df[col] = pd.to_timedelta(metrics_df[col])
        metrics_df[col] = metrics_df[col].apply(timedelta_to_string)

    return metrics_df


def get_report(
    config_file: Path, interview_name: str, report_version: str = "v1.0.0"
) -> Optional[Path]:
    """
    Returns the path to the report for the given interview name.

    Args:
        interview_name (str): The interview name.
        config_file_path (str): The path to the configuration file containing the
            PostgreSQL database credentials.

    Returns:
        Optional[Path]: The path to the report.
    """
    query = f"""
        SELECT
            pr_path
        FROM
            pdf_reports
        WHERE
            interview_name = '{interview_name}' AND
            pr_version = '{report_version}'
    """

    report_path = db.fetch_record(config_file=config_file, query=query)

    if isinstance(report_path, str):
        path = Path(report_path)
        if path.exists():
            return path
        else:
            logger.warning(f"Report does not exist: {report_path}")
            healer.remove_pdf_report(
                interview_name=interview_name, config_file=config_file
            )
            return None

    logger.warning(f"No report found for interview: {interview_name}")
    return None


def consolidate_metrics(config_file: Path) -> None:
    """
    Consolidate metrics for all studies and export to CSV and HTML.

    Args:
        config_file (Path): Path to the configuration file.
    """
    general_params = utils.config(path=config_file, section="general")
    study = general_params["study"]

    logger.info(f"Consolidating metrics for study: {study}")
    metrics_df = fetch_metrics_df(config_file=config_file, study_id=study)

    # Add column with report path
    metrics_df["report_path"] = metrics_df["interview_name"].apply(
        lambda interview_name: get_report(
            interview_name=interview_name, config_file=config_file
        )  # type: ignore
    )

    metric_params = utils.config(path=config_file, section="metrics")
    csv_path = Path(metric_params["consolidated_csv_path"])
    html_path = Path(metric_params["consolidated_html_path"])

    logger.info(f"Exporting consolidated CSV to {csv_path}")
    metrics_df.to_csv(csv_path, index=False)

    logger.info(f"Exporting trimmed data as HTML to {html_path}")

    # Drop all columns that start with:
    #  - "openface_metrics.correlations"
    #  -  "openface_metrics.std"
    #  -  "openface_metrics.mean"
    slim_metrics_df = metrics_df.loc[
        :, ~metrics_df.columns.str.startswith("openface_metrics.correlations")
    ]
    slim_metrics_df = slim_metrics_df.loc[
        :, ~slim_metrics_df.columns.str.startswith("openface_metrics.std")
    ]
    slim_metrics_df = slim_metrics_df.loc[
        :, ~slim_metrics_df.columns.str.startswith("openface_metrics.mean")
    ]
    slim_metrics_df.to_html(html_path, index=False)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="dropbox sync", description="Module to sync PDF reports to Dropbox."
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

    consolidate_metrics(config_file=config_file)

    logger.info("Done.")
