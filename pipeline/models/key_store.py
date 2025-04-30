#!/usr/bin/env python
"""
KeyStore Model
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


from pipeline.helpers import utils, db

console = utils.get_console()


class KeyStore:
    """
    Represents a key-value pair.

    Attributes:
        name (str): The name of the key.
        value (str): The value of the key.
    """

    def __init__(self, name: str, value: str) -> None:
        self.name = name
        self.value = value

    def __str__(self) -> str:
        return f"KeyStore({self.name}, {self.value})"

    def __repr__(self) -> str:
        return self.__str__()

    @staticmethod
    def init_table_query() -> str:
        """
        Return the SQL query to create the 'key_store' table.
        """
        sql_query = """
        CREATE TABLE IF NOT EXISTS key_store (
            name TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );
        """

        return sql_query

    @staticmethod
    def drop_table_query() -> str:
        """
        Return the SQL query to drop the 'key_store' table.
        """
        sql_query = """
        DROP TABLE IF EXISTS key_store;
        """

        return sql_query

    def to_sql(self) -> str:
        """
        Return the SQL query to insert the KeyStore object into the 'key_store' table.
        """
        sql_query = f"""
        INSERT INTO key_store (name, value)
        VALUES ('{self.name}', '{self.value}')
        ON CONFLICT (name) DO UPDATE SET value = '{self.value}';
        """

        return sql_query


if __name__ == "__main__":
    config_file = utils.get_config_file_path()

    console.log("Initializing 'key_store' table...")
    console.log("[red]Dropping 'key_store' table if it exists...")

    drop_queries = [KeyStore.drop_table_query()]

    create_queries = [KeyStore.init_table_query()]

    sql_queries = drop_queries + create_queries

    db.execute_queries(config_file=config_file, queries=sql_queries)

    console.log("[green]Done!")
