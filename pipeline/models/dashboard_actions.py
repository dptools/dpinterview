#!/usr/bin/env python
"""
DashboardActions Model
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

import logging
from typing import Dict, Any

from pipeline.helpers import db, utils

logger = logging.getLogger(__name__)
console = utils.get_console()


class DashboardActions:
    """
    Records all the actions performed on the dashboard (external).

    Attributes
        interview_name (str): The name of the interview.
        action (str): The action performed.
        user_id (str): The username of the user who performed the action.
        target_id (str): The target id of the action.
        target_type (str): The type of the target.
    """

    def __init__(
        self,
        interview_name: str,
        action: str,
        user_id: str,
        target_id: str,
        target_type: str,
        metadata: Dict[str, Any]
    ):
        self.interview_name = interview_name
        self.action = action
        self.user_id = user_id
        self.target_id = target_id
        self.target_type = target_type
        self.metadata = metadata

    def __repr__(self):
        return (
            f"DashboardActions("
            f"interview_name={self.interview_name}, "
            f"action={self.action}, "
            f"user_id={self.user_id}, "
            f"target_id={self.target_id}, "
            f"target_type={self.target_type})"
        )

    def __str__(self):
        return (
            f"DashboardActions: "
            f"{self.interview_name} {self.action} "
            f"{self.user_id} {self.target_id} "
            f"{self.target_type}"
        )

    @staticmethod
    def init_table_query() -> str:
        """
        Initialize the dashboard actions table.
        """
        create_table_query: str = """
            CREATE TABLE IF NOT EXISTS dashboard_actions (
                da_id SERIAL PRIMARY KEY,
                interview_name VARCHAR(255) NOT NULL,
                da_action VARCHAR(255) NOT NULL,
                da_user_id VARCHAR(255) NOT NULL,
                da_target_id VARCHAR(255),
                da_target_type VARCHAR(255),
                da_metadata JSONB,
                da_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """
        return create_table_query

    @staticmethod
    def drop_table_query() -> str:
        """
        Drop the dashboard actions table.
        """
        drop_table_query: str = """
            DROP TABLE IF EXISTS dashboard_actions;
        """
        return drop_table_query


if __name__ == "__main__":
    config_file = utils.get_config_file_path()

    console.log("Initializing 'dashboard_actions' table...")
    console.log(
        "[red]This will delete all existing data in the 'dashboard_actions' table![/red]"
    )

    drop_queries = DashboardActions.drop_table_query()
    create_queries = DashboardActions.init_table_query()

    sql_queries = [drop_queries, create_queries]

    db.execute_queries(config_file=config_file, queries=sql_queries)
    console.log("Done!")
