"""
Helper functions for reading configuration files.
"""

from configparser import ConfigParser
from pathlib import Path
from typing import Dict


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
        raise ValueError(f"Section {section} not found in the {path} file")

    return conf
