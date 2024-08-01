#!/usr/bin/env python
"""
Computing the averages and standard deviations of the FAU metrics on openface_features table
with millions of rows is very slow. This script caches the results in a cache file.

This script first gets the average and standard deviation of each FAU metric using
the DB's aggregate functions (AVG and STDDEV). Then stores the result in a CSV file.

Generated CSV File will have the cols specified in CACHE_COLS.
It will have the following format:
    col_1, col_2, col_3, ..., col_n
    avg_1, avg_2, avg_3, ..., avg_n
    std_1, std_2, std_3, ..., std_n

"""

import sys
from pathlib import Path

file = Path(__file__).resolve()
parent = file.parent
ROOT = None
for parent in file.parents:
    if parent.name == "av-pipeline-v2":
        ROOT = parent
sys.path.append(str(ROOT))

# remove current directory from path
try:
    sys.path.remove(str(parent))
except ValueError:
    pass

from typing import List, Tuple
from datetime import datetime
import logging

import pandas as pd

from pipeline.helpers import utils, cli, db
from rich.logging import RichHandler

MODULE_NAME = "cache_metrics"
INSTANCE_NAME = MODULE_NAME

console = utils.get_console()


logger = logging.getLogger(MODULE_NAME)
logargs = {
    "level": logging.DEBUG,
    # "format": "%(asctime)s - %(process)d - %(name)s - %(levelname)s - %(message)s",
    "format": "%(message)s",
    "handlers": [RichHandler(rich_tracebacks=True)],
}
logging.basicConfig(**logargs)


def get_cache_cols(test: bool = False) -> List[str]:
    """
    Returns the columns to cache.

    Returns:
    - List[str]: the columns to cache
    """

    if test:
        CACHE_COLS = ["log_id"]

        return CACHE_COLS

    AU_COLS = [
        "AU05_r",
        "AU01_r",
        "AU02_r",
        "AU06_r",
        "AU10_r",
        "AU12_r",
        "AU14_r",
        "AU15_r",
        "AU17_r",
        "AU23_r",
        "AU25_r",
        "AU26_r",
        "AU04_r",
        "AU45_r",
        "AU20_r",
        "AU07_r",
        "AU09_r",
    ]
    HEADPOSE_T_COLS = ["pose_Tx", "pose_Ty", "pose_Tz"]
    HEADPOSE_R_COLS = ["pose_Rx", "pose_Ry", "pose_Rz"]
    GAZE_COLS = ["gaze_angle_x", "gaze_angle_y"]

    CACHE_COLS = AU_COLS + HEADPOSE_T_COLS + HEADPOSE_R_COLS + GAZE_COLS

    return CACHE_COLS


def construct_query(cols: List[str], role: str) -> Tuple[str, List[str]]:
    """
    Constructs a query that gets the average and standard deviation of each FAU metric
    from openface_features table.

    Args:
    - cols (List[str]): the FAU metrics to get the average and standard deviation of

    Returns:
    - Tuple[str, List[str]]: a tuple containing the query and the columns to get
    """

    query = """
        SELECT """
    cols_in_query = []

    for col in cols:
        cols_in_query.append(f"{col}_avg")
        cols_in_query.append(f"{col}_std")

        query += f"""
            AVG("{col}") AS {col}_avg,
            STDDEV("{col}") AS {col}_std,"""

    query = query[:-1]  # remove the last comma
    query += f"""
        FROM openface_features AS openface
        WHERE openface.success = TRUE AND
            openface.ir_role = '{role}';
    """

    return query, cols_in_query


def construct_cache_file_path(role: str) -> Path:
    """
    Constructs the path to the cache file.

    Returns:
    - str: the path to the cache file
    """
    cache_file = (
        Path(cli.get_repo_root())
        / "data"
        / f"metrics_cache_{role}_{datetime.now().strftime('%Y-%m-%d')}.csv"
    )

    return cache_file


def check_if_role_exists(config_file: Path, role: str) -> bool:
    """
    Check if a role exists in the database.

    Args:
        role (str): The role to check.

    Returns:
        bool: True if the role exists, False otherwise.
    """
    query = f"""
        SELECT EXISTS (
            SELECT FROM interview_roles
            WHERE ir_role = '{role}'
        );
        """

    results = db.fetch_record(config_file=config_file, query=query)

    if results is None:
        return False

    return True


def cache_metrics(config_file: Path, role: str) -> None:
    """
    Caches the average and standard deviation of each FAU metric in a CSV file.

    Args:
    - config_file (str): path to the configuration file
    """
    cols = get_cache_cols()
    query, cols_in_query = construct_query(cols=cols, role=role)

    outputs = db.execute_queries(config_file, [query], db="openface_db")

    # Get the first output
    output = outputs[0]

    # Get the first row
    row = output[0]

    # Construct a dataframe with cols as CACHE_COLS
    df = pd.DataFrame(columns=cols)

    # Add the row to the dataframe
    for val, col in zip(row, cols_in_query):
        cache_col = col[:-4]

        # Check if avg or std
        if col.endswith("_avg"):
            df.at[0, cache_col] = val
        elif col.endswith("_std"):
            df.at[1, cache_col] = val

    # Get the path to the cache file
    cache_file = construct_cache_file_path(role=role)

    # Check if the directory exists
    if not cache_file.parent.exists():
        cache_file.parent.mkdir(parents=True)

    # Save the dataframe to the cache file
    df.to_csv(cache_file, index=False)


if __name__ == "__main__":
    config_file = utils.get_config_file_path()
    utils.configure_logging(
        config_file=config_file, module_name=MODULE_NAME, logger=logger
    )

    # Get config file path from command line
    if len(sys.argv) != 2:
        console.print(f"Usage: {sys.argv[0]} <role>")
        sys.exit(1)

    role = sys.argv[1]

    if not check_if_role_exists(config_file=config_file, role=role):
        console.print(f"[bold red]Error: Role '{role}' does not exist.")
        sys.exit(1)

    console.rule("[bold red]Caching FAU Metrics")

    with utils.get_progress_bar() as progress:
        task = progress.add_task("[green]Regenerating cache...", total=None)
        cache_metrics(config_file=config_file, role=role)

    console.log("[green]Done!")
