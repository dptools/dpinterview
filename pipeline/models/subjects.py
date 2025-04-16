"""
Subject Model
"""

from datetime import datetime
from typing import Optional
import logging

from pipeline.helpers import db

logger = logging.getLogger(__name__)


class Subject:
    """
    Represents a subject / study participant.

    Attributes:
        study_id (str): The study ID.
        subject_id (str): The subject ID.
        is_active (bool): Whether or not the subject is active.
        consent_date (datetime): The date the subject consented to the study.
        optional_notes (dict): Optional notes about the subject.
    """

    def __init__(
        self,
        study_id: str,
        subject_id: str,
        is_active: bool,
        consent_date: datetime,
        optional_notes: dict,
    ):
        self.study_id = study_id
        self.subject_id = subject_id
        self.is_active = is_active
        self.consent_date = consent_date
        self.optional_notes = optional_notes

    def __str__(self):
        return f"Subject({self.study_id}, {self.subject_id}, {self.is_active}, \
            {self.consent_date}, {self.optional_notes})"

    def __repr__(self):
        return self.__str__()

    @staticmethod
    def init_table_query() -> str:
        """
        Return the SQL query to create the 'subjects' table.
        """
        sql_query = """
        CREATE TABLE subjects (
            study_id TEXT NOT NULL REFERENCES study (study_id),
            subject_id TEXT NOT NULL,
            is_active BOOLEAN NOT NULL,
            consent_date DATE NOT NULL,
            optional_notes JSON,
            PRIMARY KEY (study_id, subject_id)
        );
        """

        return sql_query

    @staticmethod
    def drop_table_query() -> str:
        """
        Return the SQL query to drop the 'subjects' table.
        """
        sql_query = """
        DROP TABLE IF EXISTS subjects;
        """

        return sql_query

    @staticmethod
    def get_consent_date(study_id: str, subject_id: str, config_file) -> Optional[datetime]:
        """
        Return the consent date of the subject.

        Args:
            study_id (str): The study ID.
            subject_id (str): The subject ID.

        Returns:
            datetime: The consent date of the subject.
        """
        sql_query = f"""
        SELECT consent_date
        FROM subjects
        WHERE study_id = '{study_id}' AND subject_id = '{subject_id}';
        """

        result = db.fetch_record(
            query=sql_query,
            config_file=config_file,
        )

        if result is None:
            return None
        else:
            try:
                result_dt = datetime.strptime(result, "%Y-%m-%d")
            except ValueError:
                logger.error(f"Error parsing consent date {result} for {subject_id}")
                raise
            return result_dt

    def to_sql(self) -> str:
        """
        Return the SQL query to insert the subject into the 'subjects' table.
        """

        consent_date = self.consent_date.strftime("%Y-%m-%d")
        optional_notes = db.sanitize_json(self.optional_notes)

        sql_query = f"""
        INSERT INTO subjects (study_id, subject_id, is_active, consent_date, optional_notes)
        VALUES ('{self.study_id}', '{self.subject_id}', {self.is_active}, '{consent_date}', '{optional_notes}')
        ON CONFLICT(study_id, subject_id) DO UPDATE SET
            is_active = excluded.is_active,
            consent_date = excluded.consent_date,
            optional_notes = excluded.optional_notes;
        """

        return sql_query
