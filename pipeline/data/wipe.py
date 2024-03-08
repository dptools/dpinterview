"""
Functions to wipe data from the database.
"""

from pathlib import Path
from typing import List, Optional

from pipeline.helpers import db
from pipeline import data
from pipeline.models.interview_roles import InterviewRole
from pipeline.models.pdf_reports import PdfReport
from pipeline.models.load_openface import LoadOpenface
from pipeline.models.openface_qc import OpenfaceQC
from pipeline.models.openface import Openface
from pipeline.models.video_streams import VideoStream
from pipeline.models.video_qqc import VideoQuickQc
from pipeline.models.decrypted_files import DecryptedFile
from pipeline.models.metrics import Metrics
from pipeline.models.ffprobe_metadata import FfprobeMetadata


def get_interview_to_wipe(config_file: Path, study_id: str) -> Optional[str]:
    """
    Fetch a interview_name to delete from the database

    Args:
        config_file (Path): Path to config file
        study_id (str): Study ID

    Returns:
        str: interview_name
    """
    sql_query = f"""
        SELECT interviews.interview_name
        FROM decrypted_files
        INNER JOIN interview_files ON decrypted_files.source_path = interview_files.interview_file
        INNER JOIN interviews ON interview_files.interview_path = interviews.interview_path
        WHERE interviews.study_id = '{study_id}'
        ORDER BY RANDOM()
        LIMIT 1;
    """

    result = db.fetch_record(
        config_file=config_file,
        query=sql_query,
    )

    return result


def get_decrypted_files(
    config_file: Path,
    interview_name: str,
) -> List[Path]:
    """
    Returns a list of decrypted files for a given interview

    Args:
        config_file (Path): Path to the config file
        interview_name (str): Name of the interview

    Returns:
        List[Path]: List of decrypted files
    """

    sql_query = f"""
        SELECT decrypted_files.destination_path
        FROM decrypted_files
        INNER JOIN interview_files ON decrypted_files.source_path = interview_files.interview_file
        INNER JOIN interviews ON interview_files.interview_path = interviews.interview_path
        WHERE interviews.interview_name = '{interview_name}';
    """

    decrypted_files = db.execute_sql(
        config_file=config_file,
        query=sql_query,
    )
    files = decrypted_files["destination_path"].tolist()

    return [Path(file) for file in files]


def get_interview_files(
    config_file: Path,
    interview_name: str,
    version: str = "v1.0.0",
) -> List[Path]:
    """
    Get the list of interview files for the given interview name.

    Args:
        config_file: Path to the configuration file.
        interview_name: Name of the interview.

    Returns:
        List[Path]: List of interview files.
    """

    related_files: List[Path] = []
    roles = [InterviewRole.INTERVIEWER, InterviewRole.SUBJECT]

    decrypted_files = get_decrypted_files(
        config_file=config_file, interview_name=interview_name
    )

    streams: List[Path] = []
    of_paths: List[Path] = []
    for role in roles:
        stream = data.get_interview_stream(
            config_file=config_file,
            interview_name=interview_name,
            role=role,
        )

        if stream is not None:
            streams.append(stream)

        of_path = data.get_openface_path(
            config_file=config_file,
            interview_name=interview_name,
            role=role,
        )

        if of_path is not None:
            of_paths.append(of_path)

    report_path = data.get_pdf_report_path(
        config_file=config_file,
        interview_name=interview_name,
        report_version=version,
    )

    related_files.extend(decrypted_files)
    related_files.extend(streams)
    related_files.extend(of_paths)
    if report_path is not None:
        related_files.append(report_path)

    return related_files


def drop_openface_features_query(config_file: Path, interview_name: str) -> None:
    """
    Get the list of SQL queries to drop the interview data.

    Args:
        config_file: Path to the configuration file.
        interview_name: Name of the interview.

    Returns:
        str: SQL query.
    """

    sql_query = f"""
        DELETE FROM openface_features
        WHERE interview_name = '{interview_name}';
    """

    db.execute_queries(
        config_file=config_file,
        queries=[sql_query],
        db="openface_db",
    )


def drop_interview_queries(
    config_file: Path,
    interview_name: str,
    version: str = "v1.0.0",
) -> List[str]:
    """
    Get the list of SQL queries to drop the interview data.

    Args:
        config_file: Path to the configuration file.
        interview_name: Name of the interview.
        version: Version of the report.

    Returns:
        List[str]: List of SQL queries.
    """

    roles = [InterviewRole.INTERVIEWER, InterviewRole.SUBJECT]

    decrypted_files = get_decrypted_files(
        config_file=config_file, interview_name=interview_name
    )

    streams: List[Path] = []
    of_paths: List[Path] = []
    for role in roles:
        stream = data.get_interview_stream(
            config_file=config_file,
            interview_name=interview_name,
            role=role,
        )

        if stream is not None:
            streams.append(stream)

        of_path = data.get_openface_path(
            config_file=config_file,
            interview_name=interview_name,
            role=role,
        )

        if of_path is not None:
            of_paths.append(of_path)

    sql_queries = []
    drop_report_query = PdfReport.drop_row_query(
        interview_name=interview_name, pr_version=version
    )

    drop_load_openface_query = LoadOpenface.drop_row_query(
        interview_name=interview_name
    )

    drop_openface_qc_query: List[str] = []
    drop_openface_query: List[str] = []
    for of_path in of_paths:
        drop_openface_qc_query.append(
            OpenfaceQC.drop_row_query(
                of_processed_path=of_path,
            )
        )

        drop_openface_query.append(
            Openface.drop_row_query(
                of_processed_path=of_path,
            )
        )

    drop_streams_query: List[str] = []
    drop_ffprobe_query: List[str] = []
    for stream in streams:
        drop_streams_query.append(
            VideoStream.drop_row_query(
                stream_path=stream,
            )
        )
        drop_ffprobe_query.extend(
            FfprobeMetadata.drop_row_query(
                source_path=stream,
            )
        )

    drop_video_qqc_query: List[str] = []
    drop_decrypted_query: List[str] = []
    for decrypted_file in decrypted_files:
        drop_streams_query.append(
            VideoStream.drop_row_query(
                stream_path=decrypted_file,
            )
        )
        drop_video_qqc_query.append(
            VideoQuickQc.drop_row_query(
                video_path=decrypted_file,
            )
        )
        drop_ffprobe_query.extend(
            FfprobeMetadata.drop_row_query(
                source_path=decrypted_file,
            )
        )
        drop_decrypted_query.append(
            DecryptedFile.drop_row_query(
                destination_path=decrypted_file,
            )
        )

    drop_metrics_query = Metrics.drop_row_query(
        interview_name=interview_name,
    )

    sql_queries.extend(
        [
            drop_report_query,
            drop_load_openface_query,
        ],
    )
    sql_queries.extend(
        drop_openface_qc_query,
    )
    sql_queries.extend(
        drop_openface_query,
    )
    sql_queries.extend(
        drop_streams_query,
    )
    sql_queries.extend(
        drop_video_qqc_query,
    )
    sql_queries.extend(
        drop_decrypted_query,
    )
    sql_queries.extend(
        drop_ffprobe_query,
    )
    sql_queries.append(
        drop_metrics_query,
    )

    return sql_queries
