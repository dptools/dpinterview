#!/usr/bin/env python
"""
PipelineFailure Model

A durable, queryable ledger of pipeline failures - distinct from the generic
'logs' table. Deduplicates repeated failures of the same thing at the same
stage into a single row (bumping an occurrence count / last-seen timestamp)
instead of growing unboundedly, and preserves when a failure was first seen
even as it recurs.
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


from typing import Literal, Optional

from pipeline.helpers import cli, db, utils

console = utils.get_console()

IdentifierType = Literal["file_path", "study", "batch", "other"]


class PipelineFailure:
    """
    Represents a row in the 'pipeline_failures' table.

    Attributes:
        stage (str): The pipeline stage/module the failure occurred in
            (e.g. matches the module_name used for [logging] config sections).
        identifier (str): What failed - a file path when known, otherwise the
            most specific thing available (study_id, a batch description, etc).
        error (str): The error message.
        identifier_type (IdentifierType): What kind of thing `identifier` is.
        error_type (Optional[str]): The exception class name, if known.
    """

    def __init__(
        self,
        stage: str,
        identifier: str,
        error: str,
        identifier_type: IdentifierType = "file_path",
        error_type: Optional[str] = None,
    ) -> None:
        self.stage = stage
        self.identifier = identifier
        self.identifier_type = identifier_type
        self.error = error
        self.error_type = error_type

    def __str__(self) -> str:
        return f"PipelineFailure({self.stage}, {self.identifier}, {self.error})"

    def __repr__(self) -> str:
        return self.__str__()

    @staticmethod
    def init_table_query() -> str:
        """
        Return the SQL query to create the 'pipeline_failures' table.
        """
        sql_query = """
        CREATE TABLE IF NOT EXISTS pipeline_failures (
            pf_id SERIAL PRIMARY KEY,
            pf_stage TEXT NOT NULL,
            pf_identifier_type TEXT NOT NULL,
            pf_identifier TEXT NOT NULL,
            pf_error TEXT NOT NULL,
            pf_error_type TEXT,
            pf_occurrence_count INTEGER NOT NULL DEFAULT 1,
            pf_first_seen_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            pf_last_seen_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            pf_resolved BOOLEAN NOT NULL DEFAULT FALSE,
            pf_resolved_at TIMESTAMP,
            pf_resolved_note TEXT,
            UNIQUE (pf_stage, pf_identifier)
        );
        """

        return sql_query

    @staticmethod
    def drop_table_query() -> str:
        """
        Return the SQL query to drop the 'pipeline_failures' table.
        """
        sql_query = """
        DROP TABLE IF EXISTS pipeline_failures;
        """

        return sql_query

    def to_sql(self) -> str:
        """
        Return the SQL query to insert (or bump the occurrence count of) this
        PipelineFailure in the 'pipeline_failures' table.

        Note: `pf_first_seen_at` is deliberately absent from the `DO UPDATE SET`
        clause below - it must only ever be set once, by the INSERT's DEFAULT.
        Recurrence flips `pf_resolved` back to FALSE: a failure that was marked
        resolved and then happens again is not resolved anymore.
        """
        stage = db.santize_string(self.stage)
        identifier_type = db.santize_string(self.identifier_type)
        identifier = db.santize_string(self.identifier)
        error = db.santize_string(self.error)
        error_type_sql = (
            f"'{db.santize_string(self.error_type)}'"
            if self.error_type is not None
            else "NULL"
        )

        sql_query = f"""
        INSERT INTO pipeline_failures (
            pf_stage, pf_identifier_type, pf_identifier, pf_error, pf_error_type
        ) VALUES (
            '{stage}', '{identifier_type}', '{identifier}', '{error}', {error_type_sql}
        ) ON CONFLICT (pf_stage, pf_identifier) DO UPDATE SET
            pf_identifier_type = EXCLUDED.pf_identifier_type,
            pf_error = EXCLUDED.pf_error,
            pf_error_type = EXCLUDED.pf_error_type,
            pf_occurrence_count = pipeline_failures.pf_occurrence_count + 1,
            pf_last_seen_at = CURRENT_TIMESTAMP,
            pf_resolved = FALSE,
            pf_resolved_at = NULL;
        """

        return sql_query


if __name__ == "__main__":
    config_file = utils.get_config_file_path()

    console.log("Initializing 'pipeline_failures' table...")

    create_queries = [PipelineFailure.init_table_query()]  # CREATE TABLE IF NOT EXISTS

    if cli.confirm_action(
        "This table accumulates durable failure history (occurrence counts, "
        "first/last-seen timestamps). Drop and recreate 'pipeline_failures', "
        "destroying all existing failure history?"
    ):
        console.log("[red]Dropping 'pipeline_failures' table if it exists...")
        sql_queries = [PipelineFailure.drop_table_query()] + create_queries
    else:
        console.log(
            "Skipping drop. Creating the table only if it doesn't already exist "
            "(existing data, if any, is preserved)."
        )
        sql_queries = create_queries

    db.execute_queries(config_file=config_file, queries=sql_queries)

    console.log("[green]Done!")
