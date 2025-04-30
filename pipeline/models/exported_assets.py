#!/usr/bin/env python
"""
ExportedAsset Model
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

from datetime import datetime
from typing import Literal, Optional

from pipeline.helpers import db, utils

console = utils.get_console()


class ExportedAsset:
    """Represents an exported asset."""

    def __init__(
        self,
        interview_name: str,
        asset_path: Path,
        asset_type: Literal["file", "directory"],
        asset_export_type: Literal["GENERAL", "PROTECTED"],
        asset_tag: str,
        asset_destination: Path,
        aset_exported_timestamp: datetime,
    ):
        self.interview_name = interview_name
        self.asset_path = asset_path
        self.asset_type = asset_type
        self.asset_export_type = asset_export_type
        self.asset_tag = asset_tag
        self.asset_destination = asset_destination
        self.aset_exported_timestamp = aset_exported_timestamp

    def __repr__(self):
        return f"ExportedAsset({self.interview_name}, {self.asset_path}, {self.asset_tag}, \
            {self.asset_destination})"

    def __str__(self):
        return self.__repr__()

    @staticmethod
    def init_table_query() -> str:
        """
        Returns SQL query to create the `exported_assets` table.
        """
        sql_query = """
        CREATE TABLE IF NOT EXISTS exported_assets (
            interview_name TEXT NOT NULL,
            asset_path TEXT NOT NULL,
            asset_type TEXT NOT NULL,
            asset_export_type TEXT NOT NULL,
            asset_tag TEXT NOT NULL,
            asset_destination TEXT NOT NULL,
            aset_exported_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (asset_path)
        );
        """

        return sql_query

    @staticmethod
    def drop_table_query() -> str:
        """
        Returns SQL query to drop the `exported_assets` table.
        """
        sql_query = "DROP TABLE IF EXISTS exported_assets;"

        return sql_query

    def to_sql(self) -> str:
        """
        Returns SQL query to insert the object into the `exported_assets` table.
        """
        sql_query = f"""
        INSERT INTO exported_assets (
            interview_name,
            asset_path,
            asset_type,
            asset_export_type,
            asset_tag,
            asset_destination,
            aset_exported_timestamp
        ) VALUES (
            '{self.interview_name}',
            '{self.asset_path}',
            '{self.asset_type}',
            '{self.asset_export_type}',
            '{self.asset_tag}',
            '{self.asset_destination}',
            '{self.aset_exported_timestamp}'
        );
        """

        return sql_query

    @staticmethod
    def get_exported_path(local_path: Path, config_file: Path) -> Optional[Path]:
        """
        Given a local path, return the path to the exported directory.

        Note: The resulting path may require previleged access.

        Args:
            local_path (Path): The local path to the file/directory.
            config_file (Path): The path to the configuration file.

        Returns:
            Optional[Path]: The path to the exported file/directory.
                None if the path is not found.
        """

        query = f"""
        SELECT asset_destination
        FROM exported_assets
        WHERE asset_path = '{local_path}';
        """

        results = db.fetch_record(config_file=config_file, query=query)

        if results is None:
            return None

        return Path(results)


if __name__ == "__main__":
    config_file = utils.get_config_file_path()

    console.log("Initializing `exported_assets` table...")
    console.log(
        "[red]This will delete all existing data in the 'exported_assets' table![/red]"
    )

    drop_queries = [ExportedAsset.drop_table_query()]
    create_queries = [ExportedAsset.init_table_query()]

    sql_queries = drop_queries + create_queries

    db.execute_queries(config_file=config_file, queries=sql_queries)

    console.log("[green]Done!")
