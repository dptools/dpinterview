#!/usr/bin/env python
"""
Generates a HTML cover page with sortable table of metrics for the study.
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
import shutil
import tempfile
from datetime import datetime

import pandas as pd
from rich.logging import RichHandler

from pipeline.helpers import cli
from pipeline.helpers import dropbox as dropbox_helper
from pipeline.helpers import utils

MODULE_NAME = "pipeline.runners.75_generate_cover_report"
logger = logging.getLogger(MODULE_NAME)
logargs = {
    "level": logging.DEBUG,
    # "format": "%(asctime)s - %(process)d - %(name)s - %(levelname)s - %(message)s",
    "format": "%(message)s",
    "handlers": [RichHandler(rich_tracebacks=True)],
}
logging.basicConfig(**logargs)

console = utils.get_console()


def construct_title(config_file: Path, table_path: Path) -> str:
    """
    Construct the title for the cover page.

    Template: {study}-n{num_reports}-interviews-{date_str}

    Args:
        config_file (Path): Path to the configuration file.
        table_path (Path): Path to the metrics table.

    Returns:
        str: The title for the cover page.
    """
    params = utils.config(path=config_file, section="general")
    study = params["study"]

    datetime_now = datetime.now()
    date_str = datetime_now.strftime("%Y%m%d")

    df = pd.read_html(table_path)[0]
    num_reports = df.shape[0]

    return f"{study}-n{num_reports}-interviews-{date_str}"


def construct_header(title: str) -> str:
    """
    Construct the HTML header for the cover page.

    Args:
        title (str): The title for the cover page.

    Returns:
        str: The HTML header for the cover page.
    """
    return f"""
<head>
    <title>{title}</title>
    <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/handsontable/dist/handsontable.full.min.css" />
    <script type="text/javascript" src="https://cdn.jsdelivr.net/npm/handsontable/dist/handsontable.full.min.js">
    </script>
    <script src="https://cdn.jsdelivr.net/npm/moment@2.29.4/moment.min.js"></script>
</head>
"""


def construct_body(table_path: Path) -> str:
    """
    Construct the HTML body for the cover page.

    Args:
        table_path (Path): Path to the metrics table.

    Returns:
        str: The HTML body for the cover page.
    """
    table_str = open(table_path, "r", encoding="utf-8").read()

    # cut out first line
    table_str = "\n".join(table_str.split("\n")[1:])
    table_str = (
        '<table id="dataframe" class="dataframe display" style="display: none;">\n'
        + table_str
    )

    body_str = f"""
<body>
    <div id="container"></div>
    {table_str}
</body>
"""

    return body_str


def construct_script(script_path: Path) -> str:
    """
    Attach the script to the HTML.

    Args:
        script_path (Path): Path to the script.

    Returns:
        str: The script section of the HTML.
    """
    return f"""
<script type="text/javascript">
    {
        open(script_path, "r", encoding="utf-8").read()
    }
</script>
"""


def construct_html(title: str, table_path: Path, script_path: Path) -> str:
    """
    Returns the HTML for the cover page.

    Args:
        title (str): The title for the cover page.
        table_path (Path): Path to the metrics table.
        script_path (Path): Path to the script.

    Returns:
        str: The HTML for the cover page.
    """
    return f"""
<!DOCTYPE html>
<html lang="en">
{construct_header(title)}
{construct_body(table_path)}
{construct_script(script_path)}
</html>
"""


def generate_html(
    title: str, table_path: Path, script_path: Path, report_path: Path
) -> None:
    """
    Generates HTML for the cover page, and writes it to the report path.

    Args:
        title (str): The title for the cover page.
        table_path (Path): Path to the metrics table.
        script_path (Path): Path to the script.
        report_path (Path): Path to the report.

    Returns:
        None
    """
    html = construct_html(title, table_path, script_path)
    with open(report_path, "w", encoding="utf-8") as f:
        f.write(html)


def push_to_dropbox(report_path: Path, report_name: str, config_file: Path) -> None:
    """
    Push the cover report to Dropbox.

    Args:
        report_path (Path): Path to the cover report.
        config_file (Path): Path to the configuration file.

    Returns:
        None
    """
    logger.info("Pushing cover report to Dropbox...")
    dbx = dropbox_helper.get_dropbox_client(config_file)
    dropbox_params = utils.config(path=config_file, section="dropbox")
    dropbox_folder = Path(dropbox_params["reports_folder"])

    cover_dropbox_path = dropbox_folder.parent

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_report_path = Path(temp_dir) / report_name

        # Copy report to temp dir
        shutil.copy(report_path, temp_report_path)

        dropbox_helper.upload_files(dbx, [temp_report_path], str(cover_dropbox_path))

        logger.info(
            f"Cover report uploaded to Dropbox: {cover_dropbox_path}/{report_name}"
        )


def generate_cover_report(config_file: Path, dropbox_push: bool = False) -> None:
    """
    Generates a cover report with a sortable table of metrics for the study.

    Args:
        config_file (Path): Path to the configuration file.

    Returns:
        None
    """
    metric_params = utils.config(path=config_file, section="metrics")
    consolidated_html_path = Path(metric_params["consolidated_html_path"])  # HTML Table
    script_path = Path(metric_params["html_script_path"])  # JS Script for HTML Table
    report_path = Path(metric_params["html_report_path"])  # Final Report

    title = construct_title(config_file=config_file, table_path=consolidated_html_path)
    logger.info(f"Report title: {title}")

    logger.info(f"Generating HTML report at {report_path}")
    generate_html(
        title=title,
        table_path=consolidated_html_path,
        script_path=script_path,
        report_path=report_path,
    )

    if dropbox_push:
        logger.info("Pushing cover report to Dropbox...")
        push_to_dropbox(
            report_path=report_path,
            report_name=f"{title}.html",
            config_file=config_file,
        )


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

    generate_cover_report(config_file=config_file)

    logger.info("Done.")
