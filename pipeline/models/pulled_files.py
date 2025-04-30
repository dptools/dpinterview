#!/usr/bin/env python
"""
PulledFiles Model, Replaces DecryptedFile Model for AMPSCZ
"""

import sys
from pathlib import Path

file = Path(__file__).resolve()
parent = file.parent
ROOT = None
for parent in file.parents:
    if parent.name == "dpinterview":
        ROOT = parent
sys.path.append(str(ROOT))

# remove current directory from path
try:
    sys.path.remove(str(parent))
except ValueError:
    pass


from typing import Optional
from datetime import datetime

from pipeline.helpers import utils, db

console = utils.get_console()


class PulledFile:
    """Represents a pulled file."""

    def __init__(
        self,
        source_path: Path,
        destination_path: Path,
        process_time: Optional[float] = None,
        avaialble: bool = True,
        ready_to_remove: bool = False,
    ):
        self.source_path = source_path
        self.destination_path = destination_path
        self.process_time: Optional[float] = process_time
        self.pulled_at = datetime.now()
        self.available = avaialble
        self.ready_to_remove = ready_to_remove

    def __repr__(self):
        return f"PulledFile({self.source_path}, {self.destination_path})"

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
            process_time REAL,
            pulled_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            available BOOLEAN NOT NULL,
            ready_to_remove BOOLEAN NOT NULL,
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

    def to_sql(self):
        """
        Return the SQL query to insert the DecryptedFile object into the 'decrypted_files' table.
        """
        timestamp = self.pulled_at.strftime("%Y-%m-%d %H:%M:%S")
        source_path = db.santize_string(str(self.source_path))

        sql_query = f"""
        INSERT INTO decrypted_files (source_path, destination_path, \
            process_time, pulled_at, available, ready_to_remove)
        VALUES ('{source_path}', '{self.destination_path}', \
            {self.process_time}, '{timestamp}', {self.available}, {self.ready_to_remove})
        """

        return sql_query


if __name__ == "__main__":
    config_file = utils.get_config_file_path()

    console.log("Initializing 'decrypted_files' table...")
    console.log(
        "[red]This will delete all existing data in the 'decrypted_files' table![/red]"
    )

    drop_queries = [PulledFile.drop_table_query()]
    create_queries = [PulledFile.init_table_query()]

    sql_queries = drop_queries + create_queries

    db.execute_queries(config_file=config_file, queries=sql_queries)

    console.log("[green]Done!")
