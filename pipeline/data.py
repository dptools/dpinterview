from pathlib import Path
from typing import Optional, List, Dict

from pipeline.helpers import db, utils


def get_consent_date_from_subject_id(
    config_file: Path, subject_id: str, study_id
) -> Optional[str]:
    """
    Retrieves the consent date for a given subject ID and study ID.

    Args:
        config_file (Path): The path to the configuration file.
        subject_id (str): The ID of the subject.
        study_id: The ID of the study.

    Returns:
        Optional[str]: The consent date if found, None otherwise.
    """
    query = f"""
        SELECT consent_date
        FROM subjects
        WHERE subject_id = '{subject_id}' AND study_id = '{study_id}';
    """

    results = db.fetch_record(config_file=config_file, query=query)

    if results is None:
        return None
    else:
        return results


def get_subject_ids(config_file: Path, study_id: str) -> List[str]:
    """
    Gets the subject IDs from the database.

    Args:
        config_file (Path): The path to the configuration file.
        study_id (str): The study ID.

    Returns:
        List[str]: A list of subject IDs.
    """
    query = f"""
        SELECT subject_id
        FROM subjects
        WHERE study_id = '{study_id}'
        ORDER BY subject_id;
    """

    results = db.execute_sql(config_file=config_file, query=query)

    subject_ids = results["subject_id"].tolist()

    return subject_ids


def log(config_file: Path, module_name: str, message: str) -> None:
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


def get_all_cols(csv_file: Path) -> List[str]:
    """
    Returns a list of all column names in a CSV file.

    Args:
        csv_file (str): The path to the CSV file.

    Returns:
        List[str]: A list of all column names in the CSV file.
    """
    with open(csv_file, "r", encoding="utf-8") as f:
        cols = f.readline().strip().split(",")
    return cols


def get_openface_datatypes(config_file: Path, csv_file: Path) -> Dict[str, str]:
    """
    Assigns a SQL datatype to each column in the csv file.

    Args:
        config_file (str): The path to the configuration file.
        csv_file (str): The path to the CSV file.

    Returns:
        Dict[str, str]: A dictionary mapping each column name to its corresponding SQL datatype.
    """
    cols = get_all_cols(csv_file)

    params = utils.config(path=config_file, section="openface_features")

    int_cols = params["int_cols"].split(",")
    bool_cols = params["bool_cols"].split(",")
    time_cols = params["time_cols"].split(",")

    # rest of the columns are floats

    datatypes = {}

    for col in cols:
        if col in int_cols:
            datatypes[col] = "INTEGER"
        elif col in bool_cols:
            datatypes[col] = "BOOLEAN"
        elif col in time_cols:
            datatypes[col] = "TIME"
        else:
            datatypes[col] = "FLOAT"

    return datatypes
