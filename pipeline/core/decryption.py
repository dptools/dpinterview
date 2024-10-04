"""
Helper functions for decrypting files.
"""

import logging
import sys
from pathlib import Path

import cryptease as crypt

from pipeline.helpers import utils
from pipeline.helpers.config import get_key_from_config_file

logger = logging.getLogger(__name__)


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
    key = get_key_from_config_file(config_file=config_file, section="decryption")

    with open(file_to_decrypt, "rb") as f:
        key = crypt.key_from_file(f, key)

        with utils.get_progress_bar() as progress:
            progress.add_task("[cyan]Decrypting file...", total=None)
            crypt.decrypt(f, key, filename=path_for_decrypted_file)

    if not path_for_decrypted_file.exists():
        logger.error(f"Error: Decrypted file not found: {path_for_decrypted_file}")
        sys.exit(1)

    return path_for_decrypted_file
