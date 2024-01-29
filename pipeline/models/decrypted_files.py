#!/usr/bin/env python

import sys
from pathlib import Path

file = Path(__file__).resolve()
parent = file.parent
root = None
for parent in file.parents:
    if parent.name == "av-pipeline-v2":
        root = parent
sys.path.append(str(root))

# remove current directory from path
try:
    sys.path.remove(str(parent))
except ValueError:
    pass


from typing import Optional
from datetime import datetime

from pipeline.helpers import utils, db

console = utils.get_console()


class DecryptedFile:
    def __init__(
        self,
        source_path: Path,
        destination_path: Path,
        process_time: Optional[float] = None,
    ):
        self.source_path = source_path
        self.destination_path = destination_path
        self.process_time: Optional[float] = process_time
        self.decrypted_at = datetime.now()

    def __repr__(self):
        return f"DecryptedFile({self.source_path}, {self.destination_path})"

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
            decrypted_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
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
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        source_path = db.santize_string(str(self.source_path))

        sql_query = f"""
        INSERT INTO decrypted_files (source_path, destination_path, \
            process_time, decrypted_at)
        VALUES ('{source_path}', '{self.destination_path}', \
            {self.process_time}, '{timestamp}')
        """

        return sql_query


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
