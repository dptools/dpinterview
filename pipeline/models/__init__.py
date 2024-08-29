"""
Models Database tables, and contains functions to initialize the database.
"""

from pathlib import Path
from typing import List, Union

from pipeline.models.study import Study
from pipeline.models.subjects import Subject
from pipeline.models.interviews import InterviewType
from pipeline.models.interviews import Interview
from pipeline.models.files import File
from pipeline.models.interview_files import InterviewFile
from pipeline.models.key_store import KeyStore
from pipeline.models.logs import Log
from pipeline.models.decrypted_files import DecryptedFile
from pipeline.models.video_qqc import VideoQuickQc
from pipeline.models.interview_roles import InterviewRole
from pipeline.models.video_streams import VideoStream
from pipeline.models.openface import Openface
from pipeline.models.openface_qc import OpenfaceQC
from pipeline.models.load_openface import LoadOpenface
from pipeline.models.pdf_reports import PdfReport
from pipeline.models.ffprobe_metadata import FfprobeMetadata

from pipeline.helpers import db


def flatten_list(coll: list) -> list:
    """
    Flattens a list of lists into a single list.

    Args:
        coll (list): List of lists.

    Returns:
        list: Flattened list.
    """
    flat_list = []
    for i in coll:
        if isinstance(i, list):
            flat_list += flatten_list(i)
        else:
            flat_list.append(i)
    return flat_list


def init_db(config_file: Path):
    """
    Initializes the database.

    WARNING: This will drop all tables and recreate them.
    DO NOT RUN THIS IN PRODUCTION.

    Args:
        config_file (Path): Path to the config file.
    """
    drop_queries_l: List[Union[str, List[str]]] = [
        PdfReport.drop_table_query(),
        LoadOpenface.drop_table_query(),
        OpenfaceQC.drop_table_query(),
        Openface.drop_table_query(),
        VideoStream.drop_table_query(),
        InterviewRole.drop_table_query(),
        VideoQuickQc.drop_table_query(),
        DecryptedFile.drop_table_query(),
        InterviewFile.drop_table_query(),
        Interview.drop_table_query(),
        InterviewType.drop_table_query(),
        File.drop_table_query(),
        Subject.drop_table_query(),
        Study.drop_table_query(),
        KeyStore.drop_table_query(),
        Log.drop_table_query(),
        FfprobeMetadata.drop_table_query(),
    ]

    create_queries_l: List[Union[str, List[str]]] = [
        KeyStore.init_table_query(),
        Log.init_table_query(),
        Study.init_table_query(),
        Subject.init_table_query(),
        InterviewType.init_table_query(),
        Interview.init_table_query(),
        Interview.post_init_queries(),
        File.init_table_query(),
        InterviewFile.init_table_query(),
        DecryptedFile.init_table_query(),
        VideoQuickQc.init_table_query(),
        InterviewRole.init_table_query(),
        VideoStream.init_table_query(),
        Openface.init_table_query(),
        OpenfaceQC.init_table_query(),
        LoadOpenface.init_table_query(),
        PdfReport.init_table_query(),
        FfprobeMetadata.init_table_query(),
    ]

    drop_queries = flatten_list(drop_queries_l)
    create_queries = flatten_list(create_queries_l)

    sql_queries: List[str] = drop_queries + create_queries

    db.execute_queries(config_file=config_file, queries=sql_queries)
