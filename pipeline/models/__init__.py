from pathlib import Path
from typing import List, Union

from pipeline.models.study import Study
from pipeline.models.subjects import Subject
from pipeline.models.interviews import InterviewType
from pipeline.models.interviews import Interview
from pipeline.models.files import File
from pipeline.models.interview_files import InterviewFile

from pipeline.helpers import db


def flatten_list(coll: list) -> list:
    flat_list = []
    for i in coll:
        if isinstance(i, list):
            flat_list += flatten_list(i)
        else:
            flat_list.append(i)
    return flat_list


def init_db(config_file: Path):
    drop_queries: List[str] = [
        InterviewFile.drop_table_query(),
        Interview.drop_table_query(),
        InterviewType.drop_table_query(),
        File.drop_table_query(),
        Subject.drop_table_query(),
        Study.drop_table_query(),
    ]

    create_queries_l: List[Union[str, List[str]]] = [
        Study.init_table_query(),
        Subject.init_table_query(),
        InterviewType.init_table_query(),
        Interview.init_table_query(),
        File.init_table_query(),
        InterviewFile.init_table_query(),
    ]
    create_queries = flatten_list(create_queries_l)

    sql_queries: List[str] = drop_queries + create_queries

    db.execute_queries(config_file=config_file, queries=sql_queries)
