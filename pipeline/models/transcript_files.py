"""
TranscriptFiles model
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

from pipeline.helpers import db, utils

console = utils.get_console()


class TranscriptFile:
    """
    Represents a file associated with an interview.

    Attributes:
        transcript_file (Path): The path to the file.
        identifier_name (str): The name of the interview / diary.
        identifier_type (str): Is the transcript file associated with an interview or diary?
        tags (str): The tags associated with the file.
    """

    def __init__(self, transcript_file: Path, identifier_name: str, identifier_type: str, tags: str):
        self.transcript_file = transcript_file
        self.identifier_name = identifier_name
        self.identifier_type = identifier_type
        self.tags = tags

    def __str__(self):
        return f"TranscriptFile({self.transcript_file}, {self.identifier_name}, {self.identifier_type})"

    def __repr__(self):
        return self.__str__()

    @staticmethod
    def drop_table_query() -> str:
        """
        Return the SQL query to drop the 'transcript_files' table.
        """
        sql_query = """
        DROP TABLE IF EXISTS transcript_files;
        """

        return sql_query

    @staticmethod
    def init_table_query() -> str:
        """
        Return the SQL query to create the 'transcript_files' table.
        """

        sql_query = """
        CREATE TABLE IF NOT EXISTS transcript_files (
            transcript_file TEXT UNIQUE NOT NULL REFERENCES files (file_path),
            identifier_name TEXT NOT NULL,
            identifier_type TEXT NOT NULL,
            transcript_file_tags TEXT,
            PRIMARY KEY (transcript_file)
        );
        """

        return sql_query

    def to_sql(self) -> str:
        """
        Return the SQL query to insert the object into the 'transcript_files' table.
        """
        sql_query = f"""
        INSERT INTO transcript_files (transcript_file, identifier_name,
            identifier_type, transcript_file_tags)
        VALUES ('{self.transcript_file}', '{self.identifier_name}',
            '{self.identifier_type}', '{self.tags}')
        ON CONFLICT (transcript_file)
        DO UPDATE SET identifier_name = '{self.identifier_name}',
            identifier_type = '{self.identifier_type}',
            transcript_file_tags = '{self.tags}';
        """

        return sql_query


if __name__ == "__main__":
    config_file = utils.get_config_file_path()

    console.log("Initializing `transcript_files` table...")
    console.log(
        "[red]This will delete all existing data in the 'transcript_files' table![/red]"
    )

    drop_queries = [TranscriptFile.drop_table_query()]
    create_queries = [TranscriptFile.init_table_query()]

    sql_queries = drop_queries + create_queries

    db.execute_queries(config_file=config_file, queries=sql_queries)

    console.log("[green]Done!")
