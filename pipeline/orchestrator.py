"""
Pipeline ochestration module.
"""

import logging
import sys
import time
from pathlib import Path
from typing import List, Literal

from pipeline.helpers import db, cli, notifications
from pipeline.helpers.config import config

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def redirect_temp_dir(config_file: Path) -> None:
    """
    Changes the temporary directory to the one specified in the configuration file.

    Required for Singularity to work properly.

    Args:
        config_file (Path): The path to the configuration file.

    Returns:
        None
    """
    params = config(config_file, section="general")
    try:
        temp_dir = Path(params["temp_dir"])
    except KeyError:
        logger.error("Temporary directory not set. Skipping redirection...")
        return

    cli.redirect_temp_dir(temp_dir)


def get_data_root(config_file: Path, enforce_real: bool = False) -> Path:
    """
    Gets the data root directory from the configuration file.
    If fake_root is defined in the configuration file, the fake root directory is returned,
    unless enforce_real is set to True.

    Args:
        config_file (Path): The path to the configuration file.
        enforce_real (bool): If True, the real data root directory is returned.

    Returns:
        Path: The data root directory.
    """
    params = config(config_file, section="general")

    if enforce_real:
        data_root = Path(params["data_root"])
        return data_root

    try:
        fake_root = Path(params["fake_root"])
        data_root = fake_root
    except KeyError:
        data_root = Path(params["data_root"])

    return data_root


def get_templates_root(config_file: Path) -> Path:
    """
    Gets the templates root directory from the configuration file.

    Args:
        config_file (Path): The path to the configuration file.

    Returns:
        Path: The templates root directory.
    """

    params = config(config_file, section="general")
    repo_root = Path(params["repo_root"])

    templates_root = repo_root / "pipeline" / "assets" / "templates"

    return templates_root


def get_studies(config_file: Path) -> List[str]:
    """
    Gets the list of studies from configuration file.

    Args:
        config_file (Path): The path to the configuration file.

    Returns:
        List[str]: The list of studies.
    """
    params = config(path=config_file, section="general")
    studies = params["study"].split(",")

    # strip leading and trailing whitespaces
    studies = [study.strip() for study in studies]

    # Check if study metadata exists
    if not studies:
        logger.error(f"No studies found in {config_file}.")

    return studies


def translate_to_fake_root(
    config_file: Path,
    file_path: Path,
) -> Path:
    """
    Translates a file path to a fake root directory.

    Args:
        config_file (str): The path to the configuration file.
        file_path (str): The path to the file in data directory.

    Returns:
        str: The path to the file in the fake root directory.
    """

    config_params = config(config_file, section="general")
    data_root = Path(config_params["data_root"])
    fake_root = Path(config_params["fake_root"])

    fake_file_path = file_path.relative_to(data_root)
    fake_file_path = fake_root / fake_file_path

    return fake_file_path


def fix_permissions(
    config_file: Path,
    file_path: Path,
) -> None:
    """
    Fixes the permissions for a file or directory.

    Changes Ownership and Group to the user and group specified in the configuration file.

    Args:
        config_file (str): The path to the configuration file.
        file_path (str): The path to the file.

    Returns:
        None
    """
    config_params = config(config_file, section="orchestration")
    try:
        pipeline_user = config_params["pipeline_user"]
        pipeline_group = config_params["pipeline_group"]
    except KeyError:
        logger.warning("Pipeline user and group not set. Skipping permission fix...")
        return

    cli.chown(
        file_path=file_path,
        user=pipeline_user,
        group=pipeline_group,
    )

    cli.chmod(
        file_path=file_path,
        mode=770
    )


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
        logger.info(
            "[bold green]Snooze time is set to 0. Exiting...", extra={"markup": True}
        )
        sys.exit(0)

    logger.info(
        f"[bold green]No file to process. Snoozing for {snooze_time_seconds} seconds...",
        extra={"markup": True},
    )

    # Sleep for snooze_time_seconds
    # Catch KeyboardInterrupt to allow the user to stop snoozing
    try:
        time.sleep(snooze_time_seconds)
    except KeyboardInterrupt:
        try:
            logger.info("[bold red]Snooze interrupted by user.", extra={"markup": True})
            logger.info("[red]Interrupt again to exit.", extra={"markup": True})
            time.sleep(5)
        except KeyboardInterrupt:
            logger.info("[bold red]Exiting...", extra={"markup": True})
            sys.exit(0)
        logger.info("[bold green]Resuming...", extra={"markup": True})


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


def log(module_name: str, message: str, config_file: Path) -> None:
    """
    Logs a message to the database and other configured notification services.

    Args:
        module_name (str): The name of the module.
        message (str): The message to log.
        config_file (str): The path to the configuration file.

    Returns:
        None
    """
    logger.info(f"{module_name}: {message}")

    # Log to database
    db_log(config_file=config_file, module_name=module_name, message=message)
    notifications.send_notification(
        title=module_name,
        body=message,
        notify_type="info",
        config_file=config_file,
    )


def request_decrytion(config_file: Path, requester: Literal["fetch_audio", "fetch_video"]):
    """
    Requests decryption by updating the key_store table in the database.

    Args:
        config_file (str): The path to the configuration file.

    Returns:
        None
    """
    logger.info("Requesting decryption...")

    query = f"""
        UPDATE key_store
        SET value = 'enabled'
        WHERE name = '{requester}';
    """

    db.execute_queries(
        config_file,
        queries=[
            query,
        ],
        show_commands=True,
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


def complete_decryption(
    config_file: Path, requester: Literal["fetch_audio", "fetch_video"]
):
    """
    Disables decryption by updating the key_store table in the database.

    Args:
        config_file (str): The path to the configuration file.

    Returns:
        None
    """
    query = f"""
        UPDATE key_store
        SET value = 'disabled'
        WHERE name = '{requester}';
    """

    db.execute_queries(
        config_file,
        queries=[
            query,
        ],
        show_commands=False,
        silent=True,
    )


def check_if_decryption_requested(
    config_file: Path, requester: Literal["fetch_audio", "fetch_video"]
) -> bool:
    """
    Check if decryption has been requested by querying the key_store table.

    Args:
        config_file (str): The path to the configuration file.
        requester (str): The name of the module requesting decryption.

    Returns:
        bool: True if decryption has been requested, False otherwise.
    """
    message = "Checking if decryption has been requested...\t"

    query = f"""
        SELECT value
        FROM key_store
        WHERE name = '{requester}';
    """

    result = db.fetch_record(config_file=config_file, query=query)

    if result == "enabled":
        message += "[green]yes"
        logger.info(message, extra={"markup": True})
        return True
    elif result == "disabled":
        message += "[red]no"
        logger.info(message, extra={"markup": True})
        return False
    else:
        if result is None:
            message += "[yellow]no"
            logger.info(message, extra={"markup": True})
            logger.info(
                "[yellow] Initializing key_store table...", extra={"markup": True}
            )
            put_key_store(config_file, requester, "enabled")
            logger.info("[green] done", extra={"markup": True})
            return True
        else:
            message += "[red]no"
            logger.info(message, extra={"markup": True})
            logger.info(
                f"[red] Unexpected value in key_store table: {result}. Exiting...",
                extra={"markup": True},
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


def is_crawler_hashing_required(config_file: Path) -> bool:
    """
    Returns whether hashing is required for the crawler.

    Args:
        config_file (str): The path to the configuration file.

    Returns:
        bool: True if hashing is required, False otherwise.
    """
    crawler_params = config(config_file, section="crawler")
    hashing_required = crawler_params["hash_files"]

    if hashing_required == "False" or hashing_required == "false":
        return False
    else:
        return True
