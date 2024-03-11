#!/usr/bin/env python
"""
Initializes the database.
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
from typing import List, Union

from rich.logging import RichHandler

from pipeline.helpers import utils, db
from pipeline import models
from pipeline.models.study import Study
from pipeline.models.subjects import Subject
from pipeline.models.interviews import InterviewType
from pipeline.models.interviews import Interview
from pipeline.models.files import File
from pipeline.models.interview_files import InterviewFile
from pipeline.models.key_store import KeyStore
from pipeline.models.logs import Log
from pipeline.models.pulled_files import PulledFile
from pipeline.models.video_qqc import VideoQuickQc
from pipeline.models.interview_roles import InterviewRole
from pipeline.models.video_streams import VideoStream
from pipeline.models.openface import Openface
from pipeline.models.openface_qc import OpenfaceQC
from pipeline.models.load_openface import LoadOpenface
from pipeline.models.pdf_reports import PdfReport
from pipeline.models.ffprobe_metadata import FfprobeMetadata

MODULE_NAME = "init_db"
INSTANCE_NAME = MODULE_NAME

console = utils.get_console()


logger = logging.getLogger(MODULE_NAME)
logargs = {
    "level": logging.DEBUG,
    # "format": "%(asctime)s - %(process)d - %(name)s - %(levelname)s - %(message)s",
    "format": "%(message)s",
    "handlers": [RichHandler(rich_tracebacks=True)],
}
logging.basicConfig(**logargs)


def init_db(config_file: Path):
    """
    Initializes the database.

    WARNING: This will drop all tables and recreate them.
    DO NOT RUN THIS IN PRODUCTION.

    Args:
        config_file (Path): Path to the config file.
    """
    drop_queries: List[str] = [
        PdfReport.drop_table_query(),
        LoadOpenface.drop_table_query(),
        OpenfaceQC.drop_table_query(),
        Openface.drop_table_query(),
        VideoStream.drop_table_query(),
        InterviewRole.drop_table_query(),
        VideoQuickQc.drop_table_query(),
        InterviewFile.drop_table_query(),
        Interview.drop_table_query(),
        InterviewType.drop_table_query(),
        PulledFile.drop_table_query(),
        File.drop_table_query(),
        Subject.drop_table_query(),
        Study.drop_table_query(),
        KeyStore.drop_table_query(),
        Log.drop_table_query(),
    ]

    create_queries_l: List[Union[str, List[str]]] = [
        KeyStore.init_table_query(),
        Log.init_table_query(),
        Study.init_table_query(),
        Subject.init_table_query(),
        InterviewType.init_table_query(),
        Interview.init_table_query(),
        File.init_table_query(),
        InterviewFile.init_table_query(),
        PulledFile.init_table_query(),
        VideoQuickQc.init_table_query(),
        InterviewRole.init_table_query(),
        VideoStream.init_table_query(),
        Openface.init_table_query(),
        OpenfaceQC.init_table_query(),
        LoadOpenface.init_table_query(),
        PdfReport.init_table_query(),
    ]
    create_queries = models.flatten_list(create_queries_l)

    sql_queries: List[str] = drop_queries + create_queries

    db.execute_queries(config_file=config_file, queries=sql_queries)


if __name__ == "__main__":
    config_file = utils.get_config_file_path()
    utils.configure_logging(
        config_file=config_file, module_name=MODULE_NAME, logger=logger
    )

    console.rule(f"[bold red]{MODULE_NAME}")
    logger.info(f"Using config file: {config_file}")

    logger.info("Initializing database...")
    logger.warning("This will delete all existing data in the database!")

    init_db(config_file=config_file)

    logger.info("Done!")
