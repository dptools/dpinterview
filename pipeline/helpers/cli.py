import logging
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Callable, List, Optional

from pipeline.helpers.config import config


def get_repo_root() -> str:
    """
    Returns the root directory of the current Git repository.

    Uses the command `git rev-parse --show-toplevel` to get the root directory.
    """
    repo_root = subprocess.check_output(["git", "rev-parse", "--show-toplevel"])
    repo_root = repo_root.decode("utf-8").strip()
    return repo_root


def check_if_running(process_name: str) -> bool:
    """
    Check if a process with the same path is running in the background.

    Args:
        process_name (str): The name of the process to check.

    Returns:
        bool: True if the process is running, False otherwise.
    """
    command = f"ps -ef | grep -v grep | grep -c {process_name}"
    result = subprocess.run(command, stdout=subprocess.PIPE, shell=True)
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
    logger: Optional[logging.Logger] = None,
    on_fail: Callable = lambda: sys.exit(1),
) -> subprocess.CompletedProcess:
    """
    Executes a command and returns the result.

    Args:
        command_array (list): The command to execute as a list of strings.
        shell (bool, optional): Whether to execute the command in a shell. Defaults to False.
        logger (Optional[logging.Logger], optional): The logger to use for logging. Defaults to None.
        on_fail (Callable, optional): The function to call if the command fails. Defaults to lambda: sys.exit(1).

    Returns:
        subprocess.CompletedProcess: The result of the command execution.

    """

    if logger is None:
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.DEBUG)

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
        )
    else:
        result = subprocess.run(
            command_array, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )

    if result.returncode != 0:
        logger.error("=====================================")
        logger.error("Command: " + " ".join(command_array))
        logger.error("=====================================")
        logger.error("stdout:")
        logger.error(result.stdout.decode("utf-8"))
        logger.error("=====================================")
        logger.error("stderr:")
        logger.error(result.stderr.decode("utf-8"))
        logger.error("=====================================")
        logger.error("Exit code: " + str(result.returncode))
        logger.error("=====================================")

        if on_fail:
            on_fail()

    return result


def singularity_run(
    config_file: Path, command_array: list, logger: Optional[logging.Logger] = None
) -> list:
    """
    Add Singularity-specific arguments to the command.

    Args:
        config_file_path (str): The path to the configuration file.
        command_array (list): The command to run inside the container.

    Returns:
        list: The command to run inside the container, with Singularity-specific arguments added.
    """
    if logger is None:
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.DEBUG)

    params = config(path=config_file, section="singularity")
    singularity_image_path = params["singularity_image_path"]

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
        print("could not read file: " + singularity_image_path)
        sys.exit(1)

    command_array = [
        "singularity",
        "exec",
        "-B /data:/data",
        singularity_image_path,
    ] + command_array

    return command_array


def send_email(
    subject: str,
    message: str,
    recipients: List[str],
    sender: str,
    attachments: List[Path] = [],
    logger: Optional[logging.Logger] = None,
) -> None:
    """
    Send an email with the given subject and message to the given recipients.

    Uses the `mail` binary to send the email.

    Args:
        subject (str): The subject of the email.
        message (str): The message of the email.
        recipients (List[str]): The recipients of the email.
        sender (str): The sender of the email.
        attachments (List[Path], optional): The attachments to add to the email. Defaults to [].
        logger (Optional[logging.Logger], optional): The logger to use for logging. Defaults to None.

    Returns:
        None
    """

    if logger is None:
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.DEBUG)

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
        if attachments:
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
