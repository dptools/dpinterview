"""
Pipeline ochestration module.
"""

import logging
import sys
import time
from pathlib import Path

from pipeline.helpers import db
from pipeline.helpers.config import config

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def get_decryption_count(config_file: Path) -> int:
    """
    Gets the number of files to decrypt from the configuration file.

    Args:
        config_file (str): The path to the configuration file.

    Returns:
        int: The number of files to decrypt.
    """
    orchestration_params = config(config_file, section="orchestration")
    num_to_decrypt = int(orchestration_params["num_to_decrypt"])
    return num_to_decrypt


def snooze(config_file: Path) -> None:
    """
    Sleeps for a specified amount of time.

    Args:
        config_file (str): The path to the configuration file.

    Returns:
        None
    """
    params = config(config_file, section="orchestration")
    snooze_time_seconds = int(params["snooze_time_seconds"])

    if snooze_time_seconds == 0:
        logger.info("[bold green]Snooze time is set to 0. Exiting...")
        sys.exit(0)

    logger.info(
        f"[bold green]No file to process. Snoozing for {snooze_time_seconds} seconds..."
    )

    # Sleep for snooze_time_seconds
    # Catch KeyboardInterrupt to allow the user to stop snoozing
    try:
        time.sleep(snooze_time_seconds)
    except KeyboardInterrupt:
        logger.info("[bold red]Snooze interrupted by user.")
        logger.info("[red]Interrupt again to exit.")
        time.sleep(5)
        logger.info("[bold green]Resuming...")


def db_log(config_file: Path, module_name: str, message: str) -> None:
    """
    Logs a message to the database.

    Args:
    - config_file (str): the path to the configuration file
    - module_name (str): the name of the module
    - message (str): the message to log
    """
    commands = [
        f"""
        INSERT INTO logs (log_module, log_message)
        VALUES ('{module_name}', '{message}');
        """
    ]

    db.execute_queries(config_file, commands, show_commands=False, silent=True)


def request_decrytion(config_file: Path):
    """
    Requests decryption by updating the key_store table in the database.

    Args:
        config_file (str): The path to the configuration file.

    Returns:
        None
    """
    logger.info("Requesting decryption...")

    query = """
        UPDATE key_store
        SET value = 'enabled'
        WHERE name = 'decryption';
    """

    db.execute_queries(
        config_file,
        queries=[
            query,
        ],
        show_commands=False,
        silent=True,
    )


def put_key_store(config_file: Path, key: str, value: str):
    """
    Adds a key-value pair to the key_store table in the database.

    Args:
        config_file (str): The path to the configuration file.
        key (str): The key to update.
        value (str): The value to update.

    Returns:
        None
    """
    query = f"""
        INSERT INTO key_store (name, value)
        VALUES ('{key}', '{value}')
        ON CONFLICT (name) DO UPDATE SET value = '{value}';
    """

    db.execute_queries(
        config_file,
        queries=[
            query,
        ],
        show_commands=False,
        silent=True,
    )


def complete_decryption(config_file: Path):
    """
    Disables decryption by updating the key_store table in the database.

    Args:
        config_file (str): The path to the configuration file.

    Returns:
        None
    """
    query = """
        UPDATE key_store
        SET value = 'disabled'
        WHERE name = 'decryption';
    """

    db.execute_queries(
        config_file,
        queries=[
            query,
        ],
        show_commands=False,
        silent=True,
    )


def check_if_decryption_requested(config_file: Path) -> bool:
    """
    Check if decryption has been requested by querying the key_store table.

    Args:
        config_file (str): The path to the configuration file.

    Returns:
        bool: True if decryption has been requested, False otherwise.
    """
    message = "Checking if decryption has been requested...\t"

    query = """
        SELECT value
        FROM key_store
        WHERE name = 'decryption';
    """

    result = db.fetch_record(config_file=config_file, query=query)

    if result == "enabled":
        message += "[green]yes"
        logger.info(message)
        return True
    elif result == "disabled":
        message += "[red]no"
        logger.info(message)
        return False
    else:
        if result is None:
            message += "[yellow]no"
            logger.info(message)
            logger.info("[yellow] Initializing key_store table...")
            put_key_store(config_file, "decryption", "enabled")
            logger.info("[green] done")
            return True
        else:
            message += "[red]no"
            logger.info(message)
            logger.info(
                f"[red] Unexpected value in key_store table: {result}. Exiting..."
            )
            raise ValueError


def get_max_instances(
    config_file: Path,
    module_name: str,
) -> int:
    """
    Returns the maximum number of instances of a module that can be run at once.

    Args:
        config_file (str): The path to the configuration file.
        module_name (str): The name of the module.

    Returns:
        int: The maximum number of instances of a module that can be run at once.
    """
    config_params = config(config_file, section="orchestration")
    max_instances = int(config_params[f"{module_name}_max_instances"])

    return max_instances
