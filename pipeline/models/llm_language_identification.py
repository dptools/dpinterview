#!/usr/bin/env python
"""
LlmlanguageIdentification Model
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


from datetime import datetime
from typing import Any, Dict, Optional

from pydantic import BaseModel

from pipeline.helpers import utils, db

console = utils.get_console()


class LlmLanguageIdentification(BaseModel):
    """
    Language identification model.

    Attributes:
        llm_source_transcript (Path): The source transcript
        ollama_model_identifier (str): The Ollama model identifier
        llm_identified_language_code (str): The identified language code
        llm_confidence (float): The confidence score
        llm_metrics (Dict[str, Any]): The metrics
        llm_timestmap (datetime): The timestamp of when the response
            was generated.
    """

    llm_source_transcript: Path
    ollama_model_identifier: str
    llm_identified_language_code: str
    llm_confidence: float
    llm_metrics: Dict[str, Any]
    llm_task_duration_s: Optional[float] = None
    llm_timestamp: datetime = datetime.now()

    def __repr__(self):
        return f"""LlmLanguageIdentification (
    llm_source_transcript={self.llm_source_transcript},
    ollama_model_identifier={self.ollama_model_identifier},
    llm_identified_language_code={self.llm_identified_language_code},
    llm_confidence={self.llm_confidence},
    llm_metrics={self.llm_metrics},
    llm_timestamp={self.llm_timestamp}
    llm_task_duration_s={self.llm_task_duration_s}
)
"""

    def __str__(self):
        return self.__repr__()

    @staticmethod
    def init_table_query() -> str:
        """
        Return the SQL query to create the 'llm_language_identification' table.
        """
        sql_query = """
        CREATE TABLE llm_language_identification (
            llm_source_transcript TEXT NOT NULL,
            ollama_model_identifier TEXT NOT NULL,
            llm_identified_language_code TEXT,
            llm_confidence FLOAT,
            llm_metrics JSONB NOT NULL,
            llm_timestamp TIMESTAMP NOT NULL,
            llm_task_duration_s FLOAT NOT NULL,
            PRIMARY KEY (llm_source_transcript, ollama_model_identifier),
            FOREIGN KEY (llm_source_transcript) REFERENCES transcript_files (transcript_file)
        );
        """
        return sql_query

    @staticmethod
    def drop_table_query() -> str:
        """
        Return the SQL query to drop the 'llm_language_identification' table.
        """
        sql_query = """
        DROP TABLE IF EXISTS llm_language_identification;
        """
        return sql_query

    @staticmethod
    def drop_row_query(llm_source_transcript: str) -> str:
        """
        Return the SQL query to drop a row from the 'llm_language_identification' table.

        Args:
            llm_source_transcript (str): Source transcript
        """
        sql_query = f"""
        DELETE FROM llm_language_identification
        WHERE llm_source_transcript = '{llm_source_transcript}';
        """

        return sql_query

    def to_sql(self) -> str:
        """
        Return the SQL query to insert the current instance into the
        'llm_language_identification' table.
        """

        json_str = db.sanitize_json(self.llm_metrics)
        if self.llm_task_duration_s is None:
            llm_task_duration = 0
        else:
            llm_task_duration = self.llm_task_duration_s

        sql_query = f"""
        INSERT INTO llm_language_identification (
            llm_source_transcript,
            ollama_model_identifier,
            llm_identified_language_code,
            llm_confidence,
            llm_metrics,
            llm_timestamp,
            llm_task_duration_s
        ) VALUES (
            '{str(self.llm_source_transcript)}',
            '{self.ollama_model_identifier}',
            '{self.llm_identified_language_code}',
            {self.llm_confidence},
            '{json_str}',
            '{self.llm_timestamp}',
            '{llm_task_duration}'
        ) ON CONFLICT (llm_source_transcript, ollama_model_identifier) DO UPDATE
        SET
            llm_identified_language_code = '{self.llm_identified_language_code}',
            llm_confidence = {self.llm_confidence},
            llm_metrics = '{json_str}',
            llm_timestamp = '{self.llm_timestamp}',
            llm_task_duration_s = '{llm_task_duration}';
        """

        return sql_query


if __name__ == "__main__":
    config_file = utils.get_config_file_path()

    console.log("Initializing 'llm_language_identification' table...")
    console.log("[red]This will delete all existing data in the 'llm_language_identification' table!")

    drop_queries = [LlmLanguageIdentification.drop_table_query()]
    create_queries = [LlmLanguageIdentification.init_table_query()]

    sql_queries = drop_queries + create_queries

    db.execute_queries(config_file=config_file, queries=sql_queries, show_commands=True)

    console.log("'llm_language_identification' table initialized.")
