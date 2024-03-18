"""
Helper functions for decrypting files.
"""

import logging
import sys
from pathlib import Path

import cryptease as crypt

from pipeline.helpers import utils

logger = logging.getLogger(__name__)


def get_key_from_config_file(config_file: Path) -> str:
    """
    Retrieves the decryption key from the specified config file.

    Args:
        config_file (Path): The path to the config file.

    Returns:
        str: The decryption key.
    """
    # Get decryption key from config file
    params = utils.config(config_file, section="decryption")
    key_file = params["key_file"]
    logger.debug(f"Using decryption key from: {key_file}")

    # Check if key_file exists
    if not Path(key_file).exists():
        logger.error(f"Error: key_file '{key_file}' does not exist.")
        sys.exit(1)

    # Get key from key_file
    with open(key_file, "r", encoding="utf-8") as f:
        key = f.read().strip()

    return key


def decrypt_file(
    config_file: Path, file_to_decrypt: Path, path_for_decrypted_file: Path
) -> Path:
    """
    Decrypts a file using a key obtained from a configuration file.

    Args:
        config_file (str): The path to the configuration file containing the key.
        file_to_decrypt (str): The path to the file to be decrypted.
        path_for_decrypted_file (str): The path to save the decrypted file.

    Returns:
        None
    """
    # Decrypt file
    key = get_key_from_config_file(config_file=config_file)

    with open(file_to_decrypt, "rb") as f:
        key = crypt.key_from_file(f, key)

        with utils.get_progress_bar() as progress:
            progress.add_task("[cyan]Decrypting file...", total=None)
            crypt.decrypt(f, key, filename=path_for_decrypted_file)

    if not path_for_decrypted_file.exists():
        logger.error(f"Error: Decrypted file not found: {path_for_decrypted_file}")
        sys.exit(1)

    return path_for_decrypted_file
