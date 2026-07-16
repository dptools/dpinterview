#!/usr/bin/env python
"""
DatetimeOverride Model

Staff-confirmed event datetime for a raw interview file/directory whose name
could not be date-parsed by the crawler (recorded as a 'datetime_parse'
pipeline_failures row). Written by dpinterview-web's "Runsheet Match"
remediation page after a human matches the malformed file to a runsheet
entry; consumed by 2_import_interview_files.py as a fallback when the
on-disk name still fails to parse, so a manually-matched file gets imported
through the exact same downstream code path (categorization, hashing,
Interview/InterviewParts/InterviewFile creation) as a normally-named one.
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

import argparse
from datetime import datetime
from typing import Optional

from pipeline.helpers import cli, db, utils

console = utils.get_console()

# Kept alongside pipeline_failures in the same non-'public' schema - an
# override only ever exists in reference to a pipeline_failures row.
SCHEMA_NAME = "pipeline_ledger"
TABLE_NAME = f"{SCHEMA_NAME}.datetime_overrides"


class DatetimeOverride:
    """
    Represents a row in the 'pipeline_ledger.datetime_overrides' table.

    Attributes:
        identifier (str): The raw file/directory path that failed to
            date-parse - matches pipeline_failures.pf_identifier for the
            corresponding 'datetime_parse' failure.
        override_datetime (datetime): The staff-confirmed actual event
            datetime for this file.
        study_id (Optional[str]): The study this override applies to, if known.
        subject_id (Optional[str]): The subject this override applies to, if known.
    """

    def __init__(
        self,
        identifier: str,
        override_datetime: datetime,
        study_id: Optional[str] = None,
        subject_id: Optional[str] = None,
    ) -> None:
        self.identifier = identifier
        self.override_datetime = override_datetime
        self.study_id = study_id
        self.subject_id = subject_id

    def __str__(self) -> str:
        return f"DatetimeOverride({self.identifier}, {self.override_datetime})"

    def __repr__(self) -> str:
        return self.__str__()

    @staticmethod
    def init_table_query() -> str:
        """
        Return the SQL to create the 'datetime_overrides' table (schema is
        shared with, and already created by, pipeline_failures).
        """
        sql_query = f"""
        CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME};

        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            do_id SERIAL PRIMARY KEY,
            do_identifier TEXT NOT NULL UNIQUE,
            do_study_id TEXT,
            do_subject_id TEXT,
            do_override_datetime TIMESTAMP NOT NULL,
            do_created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            do_consumed_at TIMESTAMP
        );

        CREATE INDEX IF NOT EXISTS datetime_overrides_identifier_idx
            ON {TABLE_NAME} (do_identifier);
        """

        return sql_query

    @staticmethod
    def drop_table_query() -> str:
        """
        Return the SQL query to drop the 'datetime_overrides' table. Leaves
        the shared 'pipeline_ledger' schema in place.
        """
        sql_query = f"""
        DROP TABLE IF EXISTS {TABLE_NAME};
        """

        return sql_query

    def to_sql(self) -> str:
        """
        Return the SQL query to insert (or replace) this override. Re-linking
        an identifier to a new datetime clears do_consumed_at, so a
        previously-consumed override that gets corrected is picked up again
        on the next crawler pass.
        """
        identifier = db.santize_string(self.identifier)
        override_datetime = self.override_datetime.strftime("%Y-%m-%d %H:%M:%S")
        study_id_sql = (
            f"'{db.santize_string(self.study_id)}'"
            if self.study_id is not None
            else "NULL"
        )
        subject_id_sql = (
            f"'{db.santize_string(self.subject_id)}'"
            if self.subject_id is not None
            else "NULL"
        )

        sql_query = f"""
        INSERT INTO {TABLE_NAME} (
            do_identifier, do_study_id, do_subject_id, do_override_datetime
        ) VALUES (
            '{identifier}', {study_id_sql}, {subject_id_sql}, '{override_datetime}'
        ) ON CONFLICT (do_identifier) DO UPDATE SET
            do_study_id = EXCLUDED.do_study_id,
            do_subject_id = EXCLUDED.do_subject_id,
            do_override_datetime = EXCLUDED.do_override_datetime,
            do_consumed_at = NULL;
        """

        return sql_query


def get_override_datetime(config_file: Path, identifier: str) -> Optional[datetime]:
    """
    Looks up a staff-confirmed datetime override for a raw file/directory
    path that failed to date-parse, if one has been recorded and not yet
    consumed by a prior crawler pass.

    Args:
        config_file (Path): The path to the configuration file.
        identifier (str): The raw path that failed to parse (matches
            pipeline_failures.pf_identifier for the same 'datetime_parse'
            failure).

    Returns:
        Optional[datetime]: The overridden datetime, or None if no
        (unconsumed) override has been recorded for this identifier.
    """
    identifier_sql = db.santize_string(identifier)
    query = f"""
        SELECT do_override_datetime
        FROM {TABLE_NAME}
        WHERE do_identifier = '{identifier_sql}' AND do_consumed_at IS NULL;
    """
    result = db.fetch_record(config_file=config_file, query=query)
    if result is None:
        return None
    return datetime.fromisoformat(result)


def mark_consumed(config_file: Path, identifier: str) -> None:
    """
    Marks a datetime override as consumed, once a crawler pass has
    successfully used it to import the file. Best-effort and a no-op if no
    matching (unconsumed) override row exists, mirroring
    db.resolve_failure()'s call-on-every-successful-import semantics.
    """
    identifier_sql = db.santize_string(identifier)
    query = f"""
        UPDATE {TABLE_NAME}
        SET do_consumed_at = CURRENT_TIMESTAMP
        WHERE do_identifier = '{identifier_sql}' AND do_consumed_at IS NULL;
    """
    db.execute_queries(
        config_file=config_file,
        queries=[query],
        show_commands=False,
        silent=True,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="datetime_overrides",
        description="Initialize the 'pipeline_ledger.datetime_overrides' table.",
    )
    parser.add_argument(
        "-c", "--config", type=str, help="Path to the config file.", required=False
    )

    args = parser.parse_args()

    if args.config:
        config_file = Path(args.config).resolve()
        if not config_file.exists():
            console.log(f"[red]Error: Config file '{config_file}' does not exist.")
            sys.exit(1)
    else:
        config_file = utils.get_config_file_path()

    console.log("Initializing 'datetime_overrides' table...")

    create_queries = [DatetimeOverride.init_table_query()]  # CREATE TABLE IF NOT EXISTS

    if cli.confirm_action(
        "This table accumulates staff-confirmed datetime overrides for "
        "manually-matched files. Drop and recreate 'datetime_overrides', "
        "destroying all existing overrides?"
    ):
        console.log("[red]Dropping 'datetime_overrides' table if it exists...")
        sql_queries = [DatetimeOverride.drop_table_query()] + create_queries
    else:
        console.log(
            "Skipping drop. Creating the table only if it doesn't already exist "
            "(existing data, if any, is preserved)."
        )
        sql_queries = create_queries

    db.execute_queries(config_file=config_file, queries=sql_queries)

    console.log("[green]Done!")
