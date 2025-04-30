#!/usr/bin/env python
"""
Log Model
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


class Log:
    """
    Represents a row in the 'logs' table.

    Attributes:
        module_name (str): The name of the module that generated the log.
        message (str): The log message.
    """

    def __init__(self, module_name: str, message: str) -> None:
        self.module_name = module_name
        self.message = message

    def __str__(self) -> str:
        return f"Log({self.module_name}, {self.message})"

    def __repr__(self) -> str:
        return self.__str__()

    @staticmethod
    def init_table_query() -> str:
        """
        Return the SQL query to create the 'logs' table.
        """
        sql_query = """
        CREATE TABLE IF NOT EXISTS logs (
            log_id SERIAL PRIMARY KEY,
            log_module TEXT NOT NULL,
            log_message TEXT NOT NULL,
            log_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """

        return sql_query

    @staticmethod
    def drop_table_query() -> str:
        """
        Return the SQL query to drop the 'logs' table.
        """
        sql_query = """
        DROP TABLE IF EXISTS logs;
        """

        return sql_query

    def to_sql(self):
        """
        Return the SQL query to insert the Log object into the 'logs' table.
        """
        module_name = db.santize_string(self.module_name)
        message = db.santize_string(self.message)

        sql_query = f"""
        INSERT INTO logs (log_module, log_message)
        VALUES ('{module_name}', '{message}');
        """

        return sql_query


if __name__ == "__main__":
    config_file = utils.get_config_file_path()

    console.log("Initializing 'logs' table...")
    console.log("[red]This will delete all existing data in the 'logs' table![/red]")

    drop_queries = [Log.drop_table_query()]
    create_queries = [Log.init_table_query()]

    sql_queries = drop_queries + create_queries

    db.execute_queries(config_file=config_file, queries=sql_queries)

    console.log("[green]Done!")
