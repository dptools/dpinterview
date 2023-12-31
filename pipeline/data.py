from pathlib import Path
from typing import Optional, List

from pipeline.helpers import db


def get_consent_date_from_subject_id(
    config_file: Path, subject_id: str, study_id
) -> Optional[str]:
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
