"""
Helper functions for the pipeline
"""

import logging
import multiprocessing
import re
from datetime import datetime
from pathlib import Path
from typing import List

import pandas as pd
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

logger = logging.getLogger(__name__)


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
    log_file_r = log_params[module_name]

    if log_file_r.startswith("/"):
        log_file = Path(log_file_r)
    else:
        general_params = config(config_file, "general")
        repo_root = Path(general_params["repo_root"])

        log_file = repo_root / log_file_r

    if log_file.exists() and log_file.stat().st_size > 10000000:  # 10MB
        archive_file = (
            log_file.parent
            / "archive"
            / f"{log_file.stem}_{datetime.now().strftime('%Y%m%d%H%M%S')}.log"
        )
        logger.info(f"Rotating log file to {archive_file}")

        archive_file.parent.mkdir(parents=True, exist_ok=True)
        log_file.rename(archive_file)

    file_handler = logging.FileHandler(log_file, mode="a")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(
        logging.Formatter(
            "%(asctime)s  - %(process)d - %(name)s - %(levelname)s - %(message)s - [%(filename)s:%(lineno)d]"
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


def compute_relative_mean(
    group_means: pd.Series, current_means: pd.Series
) -> pd.Series:
    """
    Computes the relative mean difference between two pandas Series.

    Args:
        group_means (pd.Series): The means of a group of data.
        current_means (pd.Series): The means of the current data.

    Returns:
        pd.Series: The relative mean difference between the two input Series.
    """
    return (group_means - current_means) * -1


def datetime_time_to_float(time: datetime.time) -> float:  # type: ignore
    """
    Converts a datetime.time object to a float representing the number of seconds since midnight.

    Args:
        time (datetime.time): The time object to convert.

    Returns:
        float: The number of seconds since midnight as a float.
    """
    return time.hour * 3600 + time.minute * 60 + time.second + time.microsecond / 1e6


def create_labels(start_time: float, end_time: float, num_of_labels: int):
    """
    Creates a list of labels for a given time range and number of labels.
    E.g. create_labels(0, 60, 3) -> ["00:00", "00:30", "01:00"]

    Args:
        start_time (float): The start time of the range.
        end_time (float): The end time of the range.
        num_of_labels (int): The number of labels to create.

    Returns:
        list: A list of labels in the format "MM:SS".
    """
    interval = (end_time - start_time) / (num_of_labels - 1)

    labels = []

    for i in range(num_of_labels):
        current_time = start_time + i * interval

        minutes = int(current_time // 60)
        seconds = int(current_time % 60)

        label = f"{minutes:02d}:{seconds:02d}"

        labels.append(label)

    return labels


class FunctionTimeout(Exception):
    """
    Exception raised when a function times out.
    """


def timeout_max(seconds: int):
    """
    A decorator to set a timeout for a function.

    If the function takes longer than the specified time in seconds,
    the function will raise a FunctionTimeout exception, and terminate the running function.

    Args:
        seconds (int): The maximum time in seconds the function is allowed to run.

    Returns:
        function: The decorated function.
    """

    def decorator(func):
        def wrapper(*args, **kwargs):
            result_queue = multiprocessing.Queue()

            def target():
                try:
                    result_queue.put(func(*args, **kwargs))
                except Exception as e:
                    result_queue.put(e)

            process = multiprocessing.Process(target=target)
            process.start()
            process.join(seconds)

            if process.is_alive():
                if seconds == 0:
                    # skip the timeout
                    pass
                else:
                    process.terminate()
                process.join()
                logger.error(
                    f"Function {func.__name__} timed out after {seconds} seconds."
                )
                raise FunctionTimeout(
                    f"Function {func.__name__} timed out after {seconds} seconds."
                )

            result = result_queue.get()
            if isinstance(result, Exception):
                raise result

            return result

        return wrapper

    return decorator


def silence_logs(
    noisy_modules: List[str], target_level: int = logging.INFO
) -> None:
    """
    Silences logs from specified modules.

    Args:
        noisy_modules (List[str]): A list of modules to silence.
        target_level (int): The target log level to set the modules to.

    Returns:
        None
    """
    for module in noisy_modules:
        logger.debug(f"Setting log level for {module} to {target_level}")
        logging.getLogger(module).setLevel(target_level)
