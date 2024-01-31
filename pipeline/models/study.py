"""
Study Model
"""

from pipeline.helpers import db


class Study:
    """
    Represents a study.

    Attributes:
        study_id (str): The study ID.
    """

    def __init__(self, study_id: str):
        self.study_id = study_id

    def __str__(self):
        return f"Study({self.study_id})"

    def __repr__(self):
        return self.__str__()

    @staticmethod
    def init_table_query() -> str:
        """
        Return the SQL query to create the 'study' table.
        """
        return """
            CREATE TABLE IF NOT EXISTS study (
                study_id TEXT PRIMARY KEY
            );
        """

    @staticmethod
    def drop_table_query() -> str:
        """
        Return the SQL query to drop the 'study' table.
        """
        return """
            DROP TABLE IF EXISTS study;
        """

    def to_sql(self):
        """
        Return the SQL query to insert the object into the 'study' table.
        """
        study_id = db.santize_string(self.study_id)

        return f"""
            INSERT INTO study (study_id)
            VALUES ('{study_id}') ON CONFLICT DO NOTHING;
        """
