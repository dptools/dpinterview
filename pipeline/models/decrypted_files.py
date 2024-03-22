#!/usr/bin/env python
"""
DecryptedFile Model
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
from typing import Optional

import pandas as pd

from pipeline.helpers import db, utils

console = utils.get_console()


class DecryptedFile:
    """Represents a decrypted file."""

    def __init__(
        self,
        source_path: Path,
        destination_path: Path,
        requested_by: str,
        decrypted: bool = False,
        process_time: Optional[float] = None,
        requested_at: Optional[datetime] = datetime.now(),
        decrypted_at: Optional[datetime] = None,
    ):
        self.source_path = source_path
        self.destination_path = destination_path
        self.requested_by = requested_by
        self.decrypted = decrypted
        self.process_time = process_time
        self.requested_at = requested_at
        self.decrypted_at = decrypted_at

    def __repr__(self):
        return f"DecryptedFile({self.source_path}, {self.destination_path} {self.requested_by})"

    def __str__(self):
        return self.__repr__()

    @staticmethod
    def init_table_query() -> str:
        """
        Return the SQL query to create the 'decrypted_files' table.
        """
        sql_query = """
        CREATE TABLE IF NOT EXISTS decrypted_files (
            source_path TEXT NOT NULL REFERENCES files (file_path),
            destination_path TEXT NOT NULL UNIQUE,
            requested_by TEXT NOT NULL,
            decrypted BOOLEAN NOT NULL DEFAULT FALSE,
            process_time REAL,
            requested_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            decrypted_at TIMESTAMP DEFAULT NULL,
            PRIMARY KEY (source_path)
        );
        """

        return sql_query

    @staticmethod
    def drop_table_query() -> str:
        """
        Return the SQL query to drop the 'decrypted_files' table.
        """
        sql_query = """
        DROP TABLE IF EXISTS decrypted_files;
        """

        return sql_query

    @staticmethod
    def drop_row_query(destination_path: Path) -> str:
        """
        Return the SQL query to delete a row from the 'decrypted_files' table.

        Args:
            destination_path (Path): Destination path of the decrypted file

        Returns:
            str: SQL query to delete the row
        """

        sql_query = f"""
        DELETE FROM decrypted_files
        WHERE destination_path = '{destination_path}';
        """

        return sql_query

    def to_sql(self):
        """
        Return the SQL query to insert the DecryptedFile object into the 'decrypted_files' table.
        """
        source_path = db.santize_string(str(self.source_path))

        if self.requested_at is not None:
            requested_at = self.requested_at.strftime("%Y-%m-%d %H:%M:%S")
        else:
            requested_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        if self.decrypted_at is not None:
            decrypted_at = self.decrypted_at.strftime("%Y-%m-%d %H:%M:%S")
        else:
            decrypted_at = "NULL"

        if self.process_time is None:
            self.process_time = "NULL"

        sql_query = f"""
        INSERT INTO decrypted_files (source_path, destination_path, requested_by,
            decrypted, process_time, requested_at, decrypted_at)
        VALUES ('{source_path}', '{self.destination_path}', '{self.requested_by}',
            {self.decrypted}, {self.process_time}, '{requested_at}', {decrypted_at});
        """

        sql_query = db.handle_null(sql_query)

        return sql_query

    @staticmethod
    def get_files_pending_decrytion(
        config_file: Path,
        limit: int = 10,
    ) -> pd.DataFrame:
        """
        Get the files pending decryption.

        Args:
            config_file (Path): The path to the configuration file.
            limit (int): The maximum number of rows to return.

        Returns:
            pd.DataFrame: The files pending decryption.
        """
        sql_query = f"""
        SELECT * FROM decrypted_files
        WHERE decrypted = FALSE
        LIMIT {limit};
        """

        return db.execute_sql(
            config_file=config_file,
            query=sql_query,
        )

    @staticmethod
    def check_if_decrypted_file_exists(config_file: Path, file_path: Path) -> bool:
        """
        Checks if the decrypted file already exists.

        Args:
            config_file (Path): The path to the configuration file.
            file_path (Path): The path to the file to check.

        Returns:
            bool: True if the file exists, False otherwise.
        """
        # Check if decrypted file already exists
        sql_query = f"""
        SELECT * FROM decrypted_files
        WHERE source_path = '{file_path}';
        """

        df = db.execute_sql(
            config_file=config_file,
            query=sql_query,
        )

        return not df.empty

    @staticmethod
    def update_decrypted_status(
        config_file: Path,
        file_path: Path,
        process_time: Optional[float],
    ) -> None:
        """
        Update the decrypted status of a file.

        Args:
            config_file (Path): The path to the configuration file.
            file_path (Path): The path to the file to update.
            process_time (float): The time it took to decrypt the file.

        Returns:
            None
        """

        decrypted_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        if process_time is None:
            process_time_s = "NULL"
        else:
            process_time_s = process_time

        sql_query = f"""
        UPDATE decrypted_files
        SET decrypted = TRUE, process_time = {process_time_s}, decrypted_at = '{decrypted_at}'
        WHERE source_path = '{file_path}';
        """

        sql_query = db.handle_null(sql_query)

        db.execute_queries(config_file=config_file, queries=[sql_query])


if __name__ == "__main__":
    config_file = utils.get_config_file_path()

    console.log("Initializing 'decrypted_files' table...")
    console.log(
        "[red]This will delete all existing data in the 'decrypted_files' table![/red]"
    )

    drop_queries = [DecryptedFile.drop_table_query()]
    create_queries = [DecryptedFile.init_table_query()]

    sql_queries = drop_queries + create_queries

    db.execute_queries(config_file=config_file, queries=sql_queries)

    console.log("[green]Done!")
