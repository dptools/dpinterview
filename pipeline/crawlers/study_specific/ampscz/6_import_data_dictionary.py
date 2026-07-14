#!/usr/bin/env python
"""
Import Data Dictionary into Postgres
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
import re

import pandas as pd
from rich.logging import RichHandler

from pipeline.helpers import cli, db, utils

MODULE_NAME = "crawlers.import_data_dictionary"

console = utils.get_console()

logger = logging.getLogger(MODULE_NAME)
logargs = {
    "level": logging.DEBUG,
    # "format": "%(asctime)s - %(process)d - %(name)s - %(levelname)s - %(message)s",
    "format": "%(message)s",
    "handlers": [RichHandler(rich_tracebacks=True)],
}
logging.basicConfig(**logargs)


def remove_html_tags(input_string: str) -> str:
    """
    Remove HTML tags from a string

    Args:
        input_string (str): string with HTML tags

    Returns:
        str: string without HTML tags
    """
    if isinstance(input_string, str):
        clean_text = re.sub(r"<[^>]*>", "", input_string)
    else:
        clean_text = input_string
    return clean_text


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog=MODULE_NAME, description="Import the data dictionary into the database."
    )
    parser.add_argument(
        "-c", "--config", type=str, help="Path to the config file.", required=False
    )

    args = parser.parse_args()

    if args.config:
        config_file = Path(args.config).resolve()
        if not config_file.exists():
            console.log(f"[red]Error: Config file '{config_file}' does not exist.")
            sys.exit(1)
    else:
        if cli.confirm_action("Using default config file."):
            config_file = utils.get_config_file_path()
        else:
            sys.exit(1)

    console.rule(f"[bold red]{MODULE_NAME}")
    console.print(f"Using config file: {config_file}")

    utils.configure_logging(
        config_file=config_file, module_name=MODULE_NAME, logger=logger
    )

    data_params = utils.config(path=config_file, section="crawler")
    updated_data_dictionary_path = Path(data_params["redcap_metadata_path"])

    logger.info(f"Reading updated data dictionary from {updated_data_dictionary_path}")

    try:
        data_dictionary = pd.read_csv(updated_data_dictionary_path)

        # Remove HTML tags from all columns
        for col in data_dictionary.columns:
            data_dictionary[col] = data_dictionary[col].apply(remove_html_tags)

        db.df_to_table(
            config_file=config_file,
            df=data_dictionary,
            table_name="data_dictionary",
            if_exists="replace",
        )
    except Exception as e:
        logger.error(f"Error importing data dictionary: {e}")
        db.record_failure(
            config_file=config_file,
            stage=MODULE_NAME,
            error_code="data_dictionary_import_failed",
            identifier=str(updated_data_dictionary_path),
            error=e,
            identifier_type="file_path",
        )
        sys.exit(1)

    logger.info("Data dictionary imported successfully")
