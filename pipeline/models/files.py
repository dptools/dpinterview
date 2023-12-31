#!/usr/bin/env python

from pathlib import Path
from datetime import datetime

from pipeline.helpers import db
from pipeline.helpers.hash import compute_hash


class File:
    """
    Represents a file.

    Attributes:
        file_path (Path): The path to the file.
    """

    def __init__(
        self,
        file_path: Path,
    ):
        """
        Initialize a File object.

        Args:
            file_path (Path): The path to the file.
        """
        self.file_path = file_path

        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")

        self.file_name = file_path.name
        self.file_type = file_path.suffix
        if self.file_type == ".lock":
            # Use previous suffix for lock files
            self.file_type = file_path.suffixes[-2]

        self.file_size_mb = file_path.stat().st_size / 1024 / 1024
        self.m_time = datetime.fromtimestamp(file_path.stat().st_mtime)
        self.md5 = compute_hash(file_path=file_path, hash_type="md5")

    def __str__(self):
        """
        Return a string representation of the File object.
        """
        return f"File({self.file_name}, {self.file_type}, {self.file_size_mb}, \
            {self.file_path}, {self.m_time}, {self.md5})"

    def __repr__(self):
        """
        Return a string representation of the File object.
        """
        return self.__str__()

    @staticmethod
    def init_table_query() -> str:
        """
        Return the SQL query to create the 'files' table.
        """
        sql_query = """
        CREATE TABLE files (
            file_name TEXT NOT NULL,
            file_type TEXT NOT NULL,
            file_size_mb FLOAT NOT NULL,
            file_path TEXT PRIMARY KEY,
            m_time TIMESTAMP NOT NULL,
            md5 TEXT NOT NULL UNIQUE
        );
        """

        return sql_query

    @staticmethod
    def drop_table_query() -> str:
        """
        Return the SQL query to drop the 'files' table if it exists.
        """
        sql_query = """
        DROP TABLE IF EXISTS files;
        """

        return sql_query

    def to_sql(self):
        """
        Return the SQL query to insert the File object into the 'files' table.
        """
        f_name = db.santize_string(self.file_name)
        f_path = db.santize_string(str(self.file_path))

        sql_query = f"""
        INSERT INTO files (file_name, file_type, file_size_mb,
            file_path, m_time, md5)
        VALUES ('{f_name}', '{self.file_type}', '{self.file_size_mb}',
            '{f_path}', '{self.m_time}', '{self.md5}');
        """

        return sql_query
