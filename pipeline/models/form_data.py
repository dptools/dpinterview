#!/usr/bin/env python
"""
FormData (form_data) Model
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


from datetime import datetime
from typing import Any, Dict

from pydantic import BaseModel

from pipeline.helpers import utils, db

console = utils.get_console()


class FormData(BaseModel):
    """
    FormData model.

    Attributes:
        subject_id (str): The subject ID
        study_id (str): The study ID
        form_name (str): The form name
        event_name (str): The event name
        form_data (Dict[str, Any]): The form data
        source_mdata (datetime): The source modifed date
    """

    subject_id: str
    study_id: str
    form_name: str
    event_name: str
    form_data: Dict[str, Any]
    source_mdata: datetime

    def __repr__(self):
        return f"""FormData (
    subject_id={self.subject_id},
    form_name={self.form_name},
    event_name={self.event_name},
    form_data={self.form_data},
    source_mdata={self.source_mdata},
)"""

    @staticmethod
    def init_table_query():
        """
        Return SQL query to create the table 'form_data'.

        Returns:
            str: SQL query to create the table 'form_data'.
        """
        sql_query = """
        CREATE TABLE form_data (
            subject_id TEXT NOT NULL,
            study_id TEXT NOT NULL,
            form_name TEXT NOT NULL,
            event_name TEXT NOT NULL,
            form_data JSONB NOT NULL,
            source_mdata TIMESTAMP NOT NULL,
            imported_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (subject_id, study_id, form_name, event_name),
            FOREIGN KEY (subject_id, study_id) REFERENCES subjects (subject_id, study_id)
        );
        """

        return sql_query

    @staticmethod
    def drop_table_query():
        """
        Return SQL query to drop the table 'form_data'.

        Returns:
            str: SQL query to drop the table 'form_data'.
        """
        sql_query = """
        DROP TABLE IF EXISTS form_data;
        """

        return sql_query

    def to_sql(self):
        """
        Convert the FormData object to SQL.

        Returns:
            str: SQL query to insert the FormData object into the database.
        """

        # Convert form_data and source_mdata to JSON strings
        form_data_json = db.sanitize_json(self.form_data)

        sql_query = f"""
        INSERT INTO form_data (
            subject_id, study_id,
            form_name, event_name,
            form_data, source_mdata
        ) VALUES (
            '{self.subject_id}', '{self.study_id}',
            '{self.form_name}', '{self.event_name}',
            '{form_data_json}', '{self.source_mdata}'
        ) ON CONFLICT (subject_id, study_id, form_name, event_name) DO UPDATE SET
            form_data = EXCLUDED.form_data,
            source_mdata = EXCLUDED.source_mdata,
            imported_timestamp = CURRENT_TIMESTAMP;
        """

        return sql_query


if __name__ == "__main__":
    config_file = utils.get_config_file_path()

    console.log("Initializing 'form_data' table...")
    console.log("[red]This will delete all existing data in the 'form_data' table!")

    drop_queries = [FormData.drop_table_query()]
    create_queries = [FormData.init_table_query()]

    sql_queries = drop_queries + create_queries

    db.execute_queries(config_file=config_file, queries=sql_queries, show_commands=True)

    console.log("'form_data' table initialized.")
