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


import argparse
from typing import Literal, Optional

from pipeline.helpers import cli, db, utils

console = utils.get_console()

IdentifierType = Literal[
    "file_path", "study", "interview_name", "subject", "batch", "other"
]

# A short, stable code for *why* something failed, independent of the specific
# file/subject/message involved - e.g. every unparseable date string becomes
# "datetime_parse" instead of a one-off "invalid isoformat string: 'XYZ'" that
# can't be grouped with any other row. This is what reports/dashboards should
# group and filter by. Add new codes here as new failure sites get wired up,
# so call sites stay consistent instead of inventing near-duplicate strings.
ErrorCode = Literal[
    "datetime_parse",
    "subject_id_parse",
    "filename_parse",
    "consent_date_missing",
    "missing_file",
    "db_write_failure",
    "data_dictionary_import_failed",
    "ffprobe_streams_missing",
    "openface_datatype_cast_failed",
    "openface_load_failed",
    "decryption_failed",
    "llm_prompt_build_failed",
    "llm_language_identification_failed",
    "transcribeme_pull_failed",
    "interview_not_in_study_list",
    "other",
]

# Kept out of 'public' so a ledger row can never collide with (or be mistaken
# for) an application table, and so it can be permissioned/retained separately.
SCHEMA_NAME = "pipeline_ledger"
TABLE_NAME = f"{SCHEMA_NAME}.pipeline_failures"


class PipelineFailure:
    """
    Represents a row in the 'pipeline_ledger.pipeline_failures' table.

    Attributes:
        stage (str): The pipeline stage/module the failure occurred in
            (e.g. matches the module_name used for [logging] config sections).
        error_code (ErrorCode): A short, stable code identifying *why* this
            failed (e.g. "datetime_parse"), for grouping/reporting across
            occurrences that have different identifiers or error messages.
        identifier (str): What failed - a file path when known, otherwise the
            most specific thing available (study_id, a batch description, etc).
        error (str): The error message.
        identifier_type (IdentifierType): What kind of thing `identifier` is.
        study_id (Optional[str]): The study the failure occurred in, if known.
        subject_id (Optional[str]): The subject the failure relates to, if known.
        error_type (Optional[str]): The exception class name, if known.
    """

    def __init__(
        self,
        stage: str,
        error_code: ErrorCode,
        identifier: str,
        error: str,
        identifier_type: IdentifierType = "file_path",
        study_id: Optional[str] = None,
        subject_id: Optional[str] = None,
        error_type: Optional[str] = None,
    ) -> None:
        self.stage = stage
        self.error_code = error_code
        self.identifier = identifier
        self.identifier_type = identifier_type
        self.study_id = study_id
        self.subject_id = subject_id
        self.error = error
        self.error_type = error_type

    def __str__(self) -> str:
        return (
            f"PipelineFailure({self.stage}, {self.error_code}, "
            f"{self.identifier}, {self.error})"
        )

    def __repr__(self) -> str:
        return self.__str__()

    @staticmethod
    def init_table_query() -> str:
        """
        Return the SQL to create the 'pipeline_ledger' schema and, within it,
        the 'pipeline_failures' table - and to bring an already-existing table
        (from before pf_error_code/pf_study_id/pf_subject_id existed) up to
        the current shape via ADD COLUMN IF NOT EXISTS, so re-running this
        against an already-initialized deployment upgrades it in place instead
        of requiring a destructive drop/recreate.
        """
        sql_query = f"""
        CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME};

        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            pf_id SERIAL PRIMARY KEY,
            pf_stage TEXT NOT NULL,
            pf_error_code TEXT NOT NULL DEFAULT 'unknown',
            pf_identifier_type TEXT NOT NULL,
            pf_identifier TEXT NOT NULL,
            pf_study_id TEXT,
            pf_subject_id TEXT,
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

        ALTER TABLE {TABLE_NAME}
            ADD COLUMN IF NOT EXISTS pf_error_code TEXT NOT NULL DEFAULT 'unknown';
        ALTER TABLE {TABLE_NAME} ADD COLUMN IF NOT EXISTS pf_study_id TEXT;
        ALTER TABLE {TABLE_NAME} ADD COLUMN IF NOT EXISTS pf_subject_id TEXT;

        CREATE INDEX IF NOT EXISTS pipeline_failures_study_id_idx
            ON {TABLE_NAME} (pf_study_id);
        CREATE INDEX IF NOT EXISTS pipeline_failures_subject_id_idx
            ON {TABLE_NAME} (pf_subject_id);
        CREATE INDEX IF NOT EXISTS pipeline_failures_error_code_idx
            ON {TABLE_NAME} (pf_error_code);
        """

        return sql_query

    @staticmethod
    def drop_table_query() -> str:
        """
        Return the SQL query to drop the 'pipeline_failures' table. Leaves the
        'pipeline_ledger' schema itself in place (idempotent CREATE SCHEMA IF
        NOT EXISTS on re-init doesn't need it gone first).
        """
        sql_query = f"""
        DROP TABLE IF EXISTS {TABLE_NAME};
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
        error_code = db.santize_string(self.error_code)
        identifier_type = db.santize_string(self.identifier_type)
        identifier = db.santize_string(self.identifier)
        error = db.santize_string(self.error)
        error_type_sql = (
            f"'{db.santize_string(self.error_type)}'"
            if self.error_type is not None
            else "NULL"
        )
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
            pf_stage, pf_error_code, pf_identifier_type, pf_identifier,
            pf_study_id, pf_subject_id, pf_error, pf_error_type
        ) VALUES (
            '{stage}', '{error_code}', '{identifier_type}', '{identifier}',
            {study_id_sql}, {subject_id_sql}, '{error}', {error_type_sql}
        ) ON CONFLICT (pf_stage, pf_identifier) DO UPDATE SET
            pf_error_code = EXCLUDED.pf_error_code,
            pf_identifier_type = EXCLUDED.pf_identifier_type,
            pf_study_id = EXCLUDED.pf_study_id,
            pf_subject_id = EXCLUDED.pf_subject_id,
            pf_error = EXCLUDED.pf_error,
            pf_error_type = EXCLUDED.pf_error_type,
            pf_occurrence_count = {TABLE_NAME}.pf_occurrence_count + 1,
            pf_last_seen_at = CURRENT_TIMESTAMP,
            pf_resolved = FALSE,
            pf_resolved_at = NULL;
        """

        return sql_query


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="pipeline_failures",
        description="Initialize the 'pipeline_ledger.pipeline_failures' table.",
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
