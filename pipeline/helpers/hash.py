import hashlib
from pathlib import Path


def compute_hash(file_path: Path, hash_type: str = "md5") -> str:
    """
    Compute the hash digest of a file.

    Args:
        file_path (Path): The path to the file.
        hash_type (str, optional): The type of hash algorithm to use. Defaults to 'md5'.

    Returns:
        str: The computed hash digest of the file.
    """
    with open(file_path, "rb") as file:
        file_hash = hashlib.file_digest(file, hash_type)
        md5str = file_hash.hexdigest()

    return md5str
