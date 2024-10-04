"""
Helper functions for reading configuration files.
"""

import logging
from configparser import ConfigParser
from pathlib import Path
from typing import Dict

logger = logging.getLogger(__name__)


def config(path: Path, section: str) -> Dict[str, str]:
    """
    Read the configuration file and return a dictionary of parameters for the given section.

    Args:
        filename (str): The path to the configuration file.
        section (str): The section of the configuration file to read.

    Returns:
        dict: A dictionary of parameters for the given section.

    Raises:
        Exception: If the specified section is not found in the configuration file.
    """
    parser = ConfigParser()
    parser.read(path)

    conf = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            conf[param[0]] = param[1]
    else:
        logger.error(f"Error: Section {section} not found in the {path} file")
        raise ValueError(f"Section {section} not found in the {path} file")

    return conf


def get_key_from_config_file(config_file: Path, section: str) -> str:
    """
    Retrieves contents of key_file from the specified config files's section.

    Args:
        config_file (Path): The path to the config file.
        section (str): The section of the configuration file to read.

    Returns:
        str: The contents of the key_file.
    """
    # Get decryption key from config file
    params = config(config_file, section=section)
    try:
        key_file = params["key_file"]
    except KeyError as e:
        logger.error(
            f"Error: key_file not found in the {section} section of the {config_file} file."
        )
        raise e
    logger.debug(f"Reading {section} key (key_file) from: {key_file}")

    # Check if key_file exists
    if not Path(key_file).exists():
        logger.error(f"Error: key_file '{key_file}' does not exist.")
        raise FileNotFoundError(f"key_file '{key_file}' does not exist.")

    # Get key from key_file
    with open(key_file, "r", encoding="utf-8") as f:
        key = f.read().strip()

    return key
