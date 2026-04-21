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

try:
    sys.path.remove(str(parent))
except ValueError:
    pass

import logging
import argparse
import re

import pandas as pd
from rich.logging import RichHandler

from pipeline.helpers import cli, db, utils

MODULE_NAME = "crawlers.import_data_dictionary"

console = utils.get_console()

logger = logging.getLogger(MODULE_NAME)
logargs = {
    "level": logging.DEBUG,
    "format": "%(message)s",
    "handlers": [RichHandler(rich_tracebacks=True)],
}
logging.basicConfig(**logargs)


def remove_html_tags(input_string: str) -> str:
    """
    Remove HTML tags from a string.
    """
    if isinstance(input_string, str):
        clean_text = re.sub(r"<[^>]*>", "", input_string)
    else:
        clean_text = input_string
    return clean_text


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog=MODULE_NAME,
        description="Import REDCap Data Dictionary into the database.",
    )
    parser.add_argument(
        "-c", "--config", type=str, help="Path to the config file.", required=False
    )
    args = parser.parse_args()

    if args.config:
        config_file = Path(args.config).resolve()
        if not config_file.exists():
            logger.error(f"Config file '{config_file}' does not exist.")
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

    data_params = utils.config(path=config_file, section="crawler")
    updated_data_dictionary_path = Path(data_params["redcap_metadata_path"])

    logger.info(f"Reading data dictionary from {updated_data_dictionary_path}")

    data_dictionary = pd.read_csv(updated_data_dictionary_path)

    for col in data_dictionary.columns:
        data_dictionary[col] = data_dictionary[col].apply(remove_html_tags)

    # Rename REDCap export headers to dashboard-expected column names
    data_dictionary = data_dictionary.rename(columns={
        "Variable / Field Name":                    "field_name",
        "Form Name":                                "form_name",
        "Section Header":                           "section_header",
        "Field Type":                               "field_type",
        "Field Label":                              "field_label",
        "Choices, Calculations, OR Slider Labels":  "select_choices_or_calculations",
        "Field Note":                               "field_note",
        "Text Validation Type OR Show Slider Number": "text_validation_type_or_show_slider_number",
        "Text Validation Min":                      "text_validation_min",
        "Text Validation Max":                      "text_validation_max",
        "Identifier?":                              "identifier",
        "Branching Logic (Show field only if...)":  "branching_logic",
        "Required Field?":                          "required_field",
        "Custom Alignment":                         "custom_alignment",
        "Question Number (surveys only)":           "question_number",
        "Matrix Group Name":                        "matrix_group_name",
        "Matrix Ranking?":                          "matrix_ranking",
        "Field Annotation":                         "field_annotation",
    })
    
    db.df_to_table(
        config_file=config_file,
        df=data_dictionary,
        table_name="data_dictionary",
        if_exists="replace",
    )

    logger.info("Data dictionary imported successfully")