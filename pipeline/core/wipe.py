"""
Functions to wipe data from the database.
"""

from pathlib import Path
from typing import List, Optional
import logging

from pipeline.helpers import cli, db, utils
from pipeline import core
from pipeline.models.interview_roles import InterviewRole
from pipeline.models.pdf_reports import PdfReport
from pipeline.models.load_openface import LoadOpenface
from pipeline.models.openface_qc import OpenfaceQC
from pipeline.models.openface import Openface
from pipeline.models.video_streams import VideoStream
from pipeline.models.video_qqc import VideoQuickQc
from pipeline.models.decrypted_files import DecryptedFile
from pipeline.models.ffprobe_metadata import FfprobeMetadata

logger = logging.getLogger(__name__)


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
        ORDER BY interviews.interview_name ASC
        LIMIT 1;
    """

    result = db.fetch_record(
        config_file=config_file,
        query=sql_query,
    )

    return result


def wipe_all_interview_data(config_file: Path) -> None:
    """
    Wipe all interview data from the disk

    Args:
        config_file (Path): Path to the configuration file.

    Returns:
        None
    """

    general_params = utils.config(path=config_file, section="general")
    data_root = Path(general_params["data_root"])
    study_id = general_params["study"]

    logger.info(f"Wiping all interview data for study: {study_id}")

    interviews_dir = data_root.glob(f"PROTECTED/{study_id}/*/*_interview/processed")

    for interview_dir in interviews_dir:
        decryped_dir = interview_dir / "decrypted"
        openface_dir = interview_dir / "openface"
        reports_dir = interview_dir / "reports"

        if decryped_dir.exists():
            logger.info(f"Removing {decryped_dir}")
            cli.remove_directory(decryped_dir)
        if openface_dir.exists():
            logger.info(f"Removing {openface_dir}")
            cli.remove_directory(openface_dir)
        if reports_dir.exists():
            logger.info(f"Removing {reports_dir}")
            cli.remove_directory(reports_dir)


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
        try:
            stream = core.get_interview_stream(
                config_file=config_file,
                interview_name=interview_name,
                role=role,
            )
        except FileNotFoundError:
            stream = None
        except ValueError:
            stream = None

        if stream is not None:
            streams.append(stream)

        try:
            of_path = core.get_openface_path(
                config_file=config_file,
                interview_name=interview_name,
                role=role,
            )
        except FileNotFoundError:
            of_path = None
        except ValueError:
            of_path = None

        if of_path is not None:
            of_paths.append(of_path)

    try:
        report_path = core.get_pdf_report_path(
            config_file=config_file,
            interview_name=interview_name,
            report_version=version,
        )
    except FileNotFoundError:
        report_path = None

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


def get_video_streams(config_file: Path, video_path: Path) -> List[Path]:
    """
    Get the path to the video stream for the given interview and role.

    Args:
        config_file (Path): The path to the configuration file.
        interview_name (str): The name of the interview.
        role (InterviewRole): The role of the user.
    """

    stream_query = f"""
        SELECT vs_path
        FROM video_streams
        WHERE video_path = '{video_path}'
    """

    streams_df = db.execute_sql(
        config_file=config_file,
        query=stream_query,
    )

    stream_paths = streams_df["vs_path"].tolist()
    stream_paths = [Path(stream_path) for stream_path in stream_paths]

    return stream_paths


def get_opeface_processed_path(config_file: Path, video_stream: Path) -> Optional[Path]:
    """
    Get the path to the openface output for the given video stream.

    Args:
        config_file (Path): The path to the configuration file.
        video_stream (Path): The path to the video stream.

    Returns:
        Optional[Path]: The path to the openface output.
    """

    output_path_query = f"""
        SELECT of_processed_path
        FROM openface
        WHERE vs_path = '{video_stream}'
    """

    output_path = db.fetch_record(
        config_file=config_file,
        query=output_path_query,
    )

    if output_path is None:
        return None

    output_path = Path(output_path)

    return output_path


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

    drop_queries: List[str] = []

    decrypted_files = get_decrypted_files(
        config_file=config_file, interview_name=interview_name
    )

    drop_pdf_reports_query = PdfReport.drop_row_query(
        interview_name=interview_name, pr_version=version
    )
    drop_queries.append(drop_pdf_reports_query)

    drop_load_openface_query = LoadOpenface.drop_row_query(
        interview_name=interview_name
    )
    drop_queries.append(drop_load_openface_query)

    for file in decrypted_files:
        video_streams = get_video_streams(config_file=config_file, video_path=file)

        for stream in video_streams:
            openface_output = get_opeface_processed_path(
                config_file=config_file, video_stream=stream
            )

            if openface_output is not None:
                drop_openface_qc_query = OpenfaceQC.drop_row_query(
                    of_processed_path=openface_output
                )
                drop_queries.append(drop_openface_qc_query)

                drop_openface_query = Openface.drop_row_query(
                    of_processed_path=openface_output
                )
                drop_queries.append(drop_openface_query)

            drop_stream_query = VideoStream.drop_row_query_s(stream_path=stream)
            drop_queries.append(drop_stream_query)

            drop_stream_ffprobe_query = FfprobeMetadata.drop_row_query(
                source_path=stream
            )
            drop_queries.extend(drop_stream_ffprobe_query)

        drop_video_qqc_query = VideoQuickQc.drop_row_query(video_path=file)
        drop_queries.append(drop_video_qqc_query)

        drop_decrypted_file_query = DecryptedFile.drop_row_query(destination_path=file)
        drop_queries.append(drop_decrypted_file_query)

        drop_ffprobe_queries = FfprobeMetadata.drop_row_query(source_path=file)
        drop_queries.extend(drop_ffprobe_queries)

    return drop_queries
