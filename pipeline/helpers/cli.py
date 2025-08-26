"""
Module providing command line interface for the pipeline.
"""

import logging
import os
import random
import shutil
import signal
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


def get_repo_root_from_config(config_file: Path) -> str:
    """
    Returns the root directory of the pipeline's repository.

    Args:
        config_file (Path): The path to the configuration file.

    Returns:
        str: The root directory of the pipeline's repository.
    """

    config_params = config(path=config_file, section="general")
    repo_root = config_params["repo_root"]

    return repo_root


def create_link(
    source: Path, destination: Path, softlink: bool = True, overwrite: bool = False
) -> None:
    """
    Create a link from the source to the destination.

    Note:
    - Both source and destination must be on the same filesystem.
    - The destination must not already exist.

    Args:
        source (Path): The source of the symbolic link.
        destination (Path): The destination of the symbolic link.
        softlink (bool, optional): Whether to create a soft link.
            Defaults to True. If False, a hard link is created.

    Returns:
        None
    """
    if not source.exists():
        logger.error(f"Source path does not exist: {source}")
        raise FileNotFoundError

    if destination.exists():
        if not overwrite:
            logger.error(f"Destination path already exists: {destination}")
            raise FileExistsError
        logger.warning(f"Destination path already exists: {destination}. Overwriting.")
        remove(destination)

    dest_parent_dir = destination.parent
    if not dest_parent_dir.exists():
        logger.debug(f"Creating parent directory: {dest_parent_dir}")
        dest_parent_dir.mkdir(parents=True, exist_ok=True)

    if softlink:
        logger.debug(f"Creating soft link from {source} to {destination}")
        destination.symlink_to(source)
    else:
        logger.debug(f"Creating hard link from {source} to {destination}")
        destination.hardlink_to(source)


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
    execute_commands(
        command_array,
        shell=True,
        on_fail=lambda: logger.error("Failed to change ownership."),
    )


def chmod(file_path: Path, mode: int) -> None:
    """
    Changes the permissions of a file.

    Args:
        file_path (Path): The path to the file.
        mode (int): the mode to change the permissions to.

    Returns:
        None
    """
    command_array = ["chmod", "-R", mode, str(file_path)]
    execute_commands(
        command_array,
        shell=True,
        on_fail=lambda: logger.error("Failed to change permissions."),
    )


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


def get_process_id(process_name: str) -> Optional[List[int]]:
    """
    Get the process ID of a process with the given name.

    Args:
        process_name (str): The name of the process to get the ID of.

    Returns:
        Optional[int]: The process ID of the process, or None if the process is not running.
    """
    command = f"ps -ef | grep -v grep | grep {process_name} | awk '{{print $2}}'"
    result = subprocess.run(command, stdout=subprocess.PIPE, shell=True, check=False)
    process_ids = result.stdout.decode("utf-8").split("\n")
    process_ids = [int(x) for x in process_ids if x]
    if len(process_ids) > 1:
        logger.warning(f"Multiple processes with name {process_name} found.")
    if len(process_ids) > 0:
        return process_ids
    else:
        return None


def spawn_dummy_process(process_name: str, timeout: str = "6h") -> str:
    """
    Spawns a long running dummy process with the given name as parameter.

    Args:
        process_name (str): Part of the spawn command.
        timeout (str, optional): Timeout for the process. Defaults to '24h'.

    Returns:
        str: The unique process name.
    """

    random_str_prefix = "".join(
        random.choices("abcdefghijklmnopqrstuvwxyz0123456789", k=5)
    )

    unique_process_name = f"{random_str_prefix}_{process_name}"

    command = f"timeout {timeout} bash -c \
'while true; do echo {unique_process_name} > /dev/null; sleep 60; done'"
    subprocess.Popen(command, shell=True)

    return unique_process_name


def kill_pid(pid: int) -> None:
    """
    Kills a process with the given process ID.

    Args:
        pid (int): The process ID of the process to kill.

    Returns:
        None
    """
    try:
        os.kill(pid, signal.SIGKILL)
        # os.killpg(process_id, signal.SIGKILL)  # kill process group
    except ProcessLookupError:
        logger.warning(f"Process with ID {pid} does not exist.")
    except PermissionError:
        logger.warning(f"Permission denied to kill process with ID {pid}.")


def kill_processes(process_name: str) -> None:
    """
    Kills all processes with the given name.

    Args:
        process_name (str): The name of the process to kill.

    Returns:
        None
    """
    process_pids = get_process_id(process_name)

    logger.warning(f"Killing all processes with name {process_name} ({process_pids})")

    if process_pids is not None:
        for process_pid in process_pids:
            kill_pid(process_pid)


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


def singularity_run(
    config_file: Path, command_array: list, optional_params: Optional[str] = None
) -> list:
    """
    Add Singularity-specific arguments to the command.

    Args:
        config_file_path (str): The path to the configuration file.
        command_array (list): The command to run inside the container.
        optional_params (str, optional): Optional parameters to add to the Singularity command.
            Defaults to None.

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
        optional_params if optional_params is not None else "",
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


def remove(path: Path) -> None:
    """
    Remove a file or directory. Aso removes parent directories if they are empty.

    Args:
        path (Path): The path to the file or directory to remove.

    Returns:
        None
    """
    if path.is_dir():
        remove_directory(path)
    else:
        path.unlink()

    # Remove parent directories if they are empty
    parent = path.parent
    while parent != Path("/") and not any(parent.iterdir()):
        remove_directory(parent)
        parent = parent.parent


def copy(source: Path, destination: Path) -> None:
    """
    Copy a file or directory to a new location.

    Args:
        source (Path): The source file or directory to copy.
        destination (Path): The destination file or directory to copy to.

    Returns:
        None
    """
    if source.is_dir():
        shutil.copytree(source, destination, dirs_exist_ok=True)
    else:
        shutil.copy2(source, destination)


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
