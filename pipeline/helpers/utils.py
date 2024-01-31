"""
Helper functions for the pipeline
"""

import logging
import re
from pathlib import Path
from typing import List

from rich.console import Console
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    TaskProgressColumn,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)

from pipeline.helpers import cli
from pipeline.helpers.config import config

_console = Console(color_system="standard")


def get_progress_bar(transient: bool = False) -> Progress:
    """
    Returns a rich Progress object with standard columns.

    Returns:
        Progress: A rich Progress object with standard columns.
    """
    return Progress(
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        MofNCompleteColumn(),
        TimeElapsedColumn(),
        TimeRemainingColumn(),
        transient=transient,
    )


def get_console() -> Console:
    """
    Returns a Console object with standard color system.

    Returns:
        Console: A Console object with standard color system.
    """
    return _console


def configure_logging(config_file: Path, module_name: str, logger: logging.Logger):
    """
    Configures logging for a given module using the specified configuration file.

    Args:
        config_file (str): The path to the configuration file.
        module_name (str): The name of the module to configure logging for.
        logger (logging.Logger): The logger object to use for logging.

    Returns:
        None
    """
    log_params = config(config_file, "logging")
    log_file = log_params[module_name]

    file_handler = logging.FileHandler(log_file, mode="a")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(
        logging.Formatter(
            "%(asctime)s  - %(process)d - %(name)s - %(levelname)s - %(message)s"
        )
    )

    logging.getLogger().addHandler(file_handler)
    logger.info(f"Logging to {log_file}")


def get_config_file_path() -> Path:
    """
    Returns the path to the config file.

    Returns:
        str: The path to the config file.

    Raises:
        ConfigFileNotFoundExeption: If the config file is not found.
    """
    repo_root = cli.get_repo_root()
    config_file_path = repo_root + "/config.ini"

    # Check if config_file_path exists
    if not Path(config_file_path).is_file():
        raise FileNotFoundError(f"Config file not found at {config_file_path}")

    return Path(config_file_path)


def get_instance_name(module_name: str, process_name: str) -> str:
    """
    Returns the process name, with the number of instances of the process running
    appended to the end.

    Args:
        module_name (str): The name of the module eg. decrytion
        process_name (str): The name of the process. eg. 01_decryption.py

    Returns:
        str: The process name, with the number of instances of the process running
        appended to the end.
    """

    # Get the number of instances of the process running
    instance_id = cli.get_number_of_running_processes(process_name)

    # Append the number of instances of the process running to the end of the process name
    module_name = module_name + "_" + str(instance_id)

    return module_name


def camel_case_split(word: str) -> List[str]:
    """
    Splits a camel case word into a list of lowercase words.

    Args:
        word (str): The camel case word to be split.

    Returns:
        List[str]: A list of lowercase words.

    Example:
        >>> camel_case_split("camelCaseWord")
        ['camel', 'case', 'word']
    """
    # If first character is lowercase make it uppercase
    word = word[0].upper() + word[1:]
    parts: List[str] = re.findall(r"[A-Z](?:[a-z]+|[A-Z]*(?=[A-Z]|$))", word)

    # make all parts lowercase
    parts = [part.lower() for part in parts]

    return parts
