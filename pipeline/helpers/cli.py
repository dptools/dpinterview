"""
Module providing command line interface for the pipeline.
"""

import logging
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Callable, List, Optional

from pipeline.helpers.config import config

logger = logging.getLogger(__name__)


def get_repo_root() -> str:
    """
    Returns the root directory of the current Git repository.

    Uses the command `git rev-parse --show-toplevel` to get the root directory.
    """
    repo_root = subprocess.check_output(["git", "rev-parse", "--show-toplevel"])
    repo_root = repo_root.decode("utf-8").strip()
    return repo_root


def redirect_temp_dir(new_temp_dir: Path) -> None:
    """
    Changes the temporary directory to the given directory.

    Required for Singularity to work properly.

    Args:
        new_temp_dir (Path): The new temporary directory to use.

    Returns:
        None
    """

    # Set the temporary directory
    temp_dir = new_temp_dir
    temp_dir.mkdir(parents=True, exist_ok=True)
    temp_dir_str = str(temp_dir)

    # Set the environment variable
    os.environ["TMPDIR"] = temp_dir_str
    os.environ["TEMP"] = temp_dir_str
    os.environ["TMP"] = temp_dir_str
    os.environ["TMPDIR"] = temp_dir_str

    logger.debug("Temporary directory set to: %s", temp_dir_str)


def set_environment_variable(variable: str, value: str, overwrite: bool = True) -> None:
    """
    Set an environment variable.

    Args:
        variable (str): The name of the environment variable to set.
        value (str): The value to set the environment variable to.
        overwrite (bool, optional): Whether to overwrite the environment variable
            if it already exists.
            Defaults to True.

    Returns:
        None
    """
    if overwrite or variable not in os.environ:
        os.environ[variable] = value
        logger.debug(f"Environment variable set: {variable}={value}")
    else:
        logger.debug(
            f"Environment variable not set (already exists): {variable}={os.environ[variable]}"
        )


def chown(file_path: Path, user: str, group: str) -> None:
    """
    Changes the ownership of a file.

    Args:
        file_path (Path): The path to the file.
        user (str): The user to change the ownership to.
        group (str): The group to change the ownership to.

    Returns:
        None
    """
    command_array = ["chown", "-R", f"{user}:{group}", str(file_path)]
    execute_commands(command_array, shell=True)


def check_if_running(process_name: str) -> bool:
    """
    Check if a process with the same path is running in the background.

    Args:
        process_name (str): The name of the process to check.

    Returns:
        bool: True if the process is running, False otherwise.
    """
    command = f"ps -ef | grep -v grep | grep -c {process_name}"
    result = subprocess.run(command, stdout=subprocess.PIPE, shell=True, check=False)
    num_processes = int(result.stdout.decode("utf-8"))
    return num_processes > 0


def get_number_of_running_processes(process_name: str) -> int:
    """
    Returns the number of instances of a process running with the given name.

    Args:
        process_name (str): The name of the process to search for.

    Returns:
        int: The number of instances of the process running.
    """
    # Get the number of instances of the process running
    command_array = [
        "ps",
        "-ef",
        "|",
        "grep",
        process_name,
        "|",
        "grep",
        "-v",
        "grep",
        "|",
        "wc",
        "-l",
    ]

    # Run the command
    output = execute_commands(
        command_array=command_array,
        shell=True,
    )

    # Get the number of processes
    num_processes = int(output.stdout.decode("utf-8"))

    return num_processes


def execute_commands(
    command_array: list,
    shell: bool = False,
    on_fail: Callable = lambda: sys.exit(1),
) -> subprocess.CompletedProcess:
    """
    Executes a command and returns the result.

    Args:
        command_array (list): The command to execute as a list of strings.
        shell (bool, optional): Whether to execute the command in a shell. Defaults to False.
        logger (Optional[logging.Logger], optional): The logger to use for logging.
            Defaults to None.
        on_fail (Callable, optional): The function to call if the command fails.
            Defaults to lambda: sys.exit(1).

    Returns:
        subprocess.CompletedProcess: The result of the command execution.

    """
    logger.debug("Executing command:")
    # cast to str to avoid error when command_array is a list of Path objects
    command_array = [str(x) for x in command_array]

    if logger:
        logger.debug(" ".join(command_array))

    if shell:
        result = subprocess.run(
            " ".join(command_array),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            shell=True,
            check=False,
        )
    else:
        result = subprocess.run(
            command_array, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False
        )

    if result.returncode != 0:
        logger.error("=====================================")
        logger.error("Command: %s", " ".join(command_array))
        logger.error("=====================================")
        logger.error("stdout:")
        logger.error(result.stdout.decode("utf-8"))
        logger.error("=====================================")
        logger.error("stderr:")
        logger.error(result.stderr.decode("utf-8"))
        logger.error("=====================================")
        logger.error("Exit code: %s", str(result.returncode))
        logger.error("=====================================")

        if on_fail:
            on_fail()

    return result


def singularity_run(config_file: Path, command_array: list) -> list:
    """
    Add Singularity-specific arguments to the command.

    Args:
        config_file_path (str): The path to the configuration file.
        command_array (list): The command to run inside the container.

    Returns:
        list: The command to run inside the container, with Singularity-specific arguments added.
    """
    params = config(path=config_file, section="singularity")
    singularity_image_path = params["singularity_image_path"]
    bind_params = params["bind_params"]

    # Check if singularity binary exists
    if shutil.which("singularity") is None:
        logger.error(
            "[red][u]singularity[/u] binary not found.[/red]", extra={"markup": True}
        )
        logger.error(
            "[yellow]Did you run '[i]module load singularity'[/i]?",
            extra={"markup": True},
        )
        sys.exit(1)

    # Check if singularity_image_path exists
    if not Path(singularity_image_path).is_file():
        logger.error(f"Could not read file: {singularity_image_path}")
        sys.exit(1)

    command_array = [
        "singularity",
        "exec",
        f"-B {bind_params}",
        singularity_image_path,
    ] + command_array

    return command_array


def send_email(
    subject: str,
    message: str,
    recipients: List[str],
    sender: str,
    attachments: Optional[List[Path]] = None,
) -> None:
    """
    Send an email with the given subject and message to the given recipients.

    Uses the `mail` binary to send the email.

    Args:
        subject (str): The subject of the email.
        message (str): The message of the email.
        recipients (List[str]): The recipients of the email.
        sender (str): The sender of the email.
        attachments (List[Path], optional): The attachments to add to the email.
            Defaults to None.

    Returns:
        None
    """
    if shutil.which("mail") is None:
        logger.error("[red][u]mail[/u] binary not found.[/red]", extra={"markup": True})
        logger.warning(
            "[yellow]Skipping sending email.[/yellow]", extra={"markup": True}
        )
        return

    with tempfile.NamedTemporaryFile(mode="w", prefix="email_", suffix=".eml") as temp:
        temp.write(f"From: {sender}\n")
        temp.write(f"To: {', '.join(recipients)}\n")
        temp.write(f"Subject: {subject}\n")
        temp.write("\n")
        temp.write(message)
        temp.write("\n")
        if attachments is not None:
            temp.write("\n")
            temp.write(f"{len(attachments)} Attachment(s):\n")
            for attachment in attachments:
                temp.write(str(attachment.name) + "\n")
        temp.flush()

        command_array = [
            "mail",
            "-s",
            f"'{subject}'",  # wrap subject in quotes to avoid issues with special characters
        ]

        if attachments is not None:
            for attachment in attachments:
                command_array += ["-a", str(attachment)]

        command_array += recipients

        command_array += ["<", temp.name]

        logger.debug("Sending email:")
        logger.debug(" ".join(command_array))
        execute_commands(command_array, shell=True)


def remove_directory(path: Path) -> None:
    """
    Remove all files in the specified directory.

    Args:
        path (str): The path to the directory to be cleared.

    Returns:
        None
    """

    # Check if directory exists
    if not Path(path).is_dir():
        return

    shutil.rmtree(path)


def confirm_action(message: str) -> bool:
    """
    Ask the user to confirm an action.

    Args:
        message (str): The message to display to the user.

    Returns:
        bool: True if the user confirms the action, False otherwise.
    """
    logger.warning(message)
    user_input = input("Do you want to continue? (yes/no): ")

    result = user_input.lower() == "yes" or user_input.lower() == "y"

    return result
