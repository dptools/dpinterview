#!/usr/bin/env python
"""
Runs OpenFace on video streams
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
from typing import Optional, Tuple
import os

from rich.logging import RichHandler

from pipeline import orchestrator, data
from pipeline.helpers import utils, db, dpdash, cli
from pipeline.helpers.timer import Timer
from pipeline.models.interview_roles import InterviewRole
from pipeline.models.openface import Openface

MODULE_NAME = "openface"
INSTANCE_NAME = MODULE_NAME

logger = logging.getLogger(MODULE_NAME)
logargs = {
    "level": logging.DEBUG,
    # "format": "%(asctime)s - %(process)d - %(name)s - %(levelname)s - %(message)s",
    "format": "%(message)s",
    "handlers": [RichHandler(rich_tracebacks=True)],
}
logging.basicConfig(**logargs)

console = utils.get_console()


def get_file_to_process(
    config_file: Path, study_id: str
) -> Optional[Tuple[Path, InterviewRole, Path]]:
    """
    Fetch a file to process from the database.

    - Fetches a file that has not been processed yet and is part of the study.
        - Must have completed split-streams process

    Args:
        config_file (Path): Path to config file
        study_id (str): Study ID

    Returns:
        Optional[Tuple[Path, InterviewRole, Path]]: Tuple of video stream path,
            interview role, and video path
    """
    sql_query = f"""
        SELECT vs.vs_path, vs.ir_role, vs.video_path
        FROM video_streams AS vs
        INNER JOIN (
            SELECT decrypted_files.destination_path, interview_files.interview_file_tags
            FROM interview_files JOIN decrypted_files
            ON interview_files.interview_file = decrypted_files.source_path
        ) AS if
        ON vs.video_path = if.destination_path
        WHERE vs.vs_path NOT IN (
            SELECT vs_path FROM openface
        ) AND vs.video_path IN (
            SELECT destination_path FROM decrypted_files
            JOIN interview_files ON interview_files.interview_file = decrypted_files.source_path
            JOIN interviews USING (interview_path)
            WHERE interviews.study_id = '{study_id}'
        )
        ORDER BY RANDOM()
        LIMIT 1;
    """

    result_df = db.execute_sql(config_file=config_file, query=sql_query)
    if result_df.empty:
        return None

    video_stream_path = Path(result_df.iloc[0]["vs_path"])
    interview_role: InterviewRole = InterviewRole.from_str(result_df.iloc[0]["ir_role"])
    video_path = Path(result_df.iloc[0]["video_path"])

    return video_stream_path, interview_role, video_path


def get_other_stream_to_process(
    config_file: Path, video_path: Path
) -> Optional[Tuple[Path, InterviewRole, Path]]:
    """
    Fetch a file to process from the database.

    - Fetches a file that has not been processed yet and
        is part of the study.
    - Fetches video stream that is part of the same video as the
        video stream that was just processed.

    Args:
        config_file (Path): Path to config file

    Returns:
        Optional[Tuple[Path, InterviewRole, Path]]: Tuple of video stream path,
            interview role, and video path
    """
    sql_query = f"""
        SELECT vs.vs_path, vs.ir_role, vs.video_path
        FROM video_streams AS vs
        WHERE NOT EXISTS (
            SELECT * FROM openface
            WHERE openface.vs_path = vs.vs_path AND
            openface.ir_role = vs.ir_role
        ) AND vs.video_path = '{video_path}';
        """

    result_df = db.execute_sql(config_file=config_file, query=sql_query)
    if result_df.empty:
        return None

    video_stream_path = Path(result_df.iloc[0]["vs_path"])
    interview_role: InterviewRole = InterviewRole.from_str(result_df.iloc[0]["ir_role"])
    video_path = Path(result_df.iloc[0]["video_path"])

    return video_stream_path, interview_role, video_path


def construct_output_path(config_file: Path, video_path: Path) -> Path:
    """
    Construct output path for OpenFace output

    Args:
        config_file (Path): Path to config file
        video_path (Path): Path to video
    """
    config_params = utils.config(path=config_file, section="general")
    data_root = Path(config_params["data_root"])

    base_name = Path(video_path).name
    # Remove extension
    base_name = Path(base_name).with_suffix("")

    dp_dash_dict = dpdash.parse_dpdash_name(str(base_name))
    dp_data_type = dp_dash_dict["data_type"]

    if dp_data_type is None:
        logger.error(f"Could not parse data type from {base_name}")
        raise ValueError(f"Could not parse data type from {base_name}")

    data_type_parts = utils.camel_case_split(dp_data_type)  # type: ignore
    data_type = "_".join(data_type_parts)

    output_path = Path(
        data_root,
        "PROTECTED",
        dp_dash_dict["study"],  # type: ignore
        dp_dash_dict["subject"],  # type: ignore
        data_type,
        "processed",
        "openface",
        base_name,
    )

    if not output_path.exists():
        output_path.mkdir(parents=True, exist_ok=True)
    else:
        # Check if empty
        if len(list(output_path.iterdir())) > 0:
            logger.warning(f"Output path already exists: {output_path}")
            # Add number to end of path
            base_output_path = output_path
            i = 1
            while output_path.exists():
                output_path = Path(f"{base_output_path}_{i}")
                i += 1

    return output_path


def run_openface(
    config_file: Path, file_path_to_process: Path, output_path: Path
) -> None:
    """
    Run OpenFace on a video stream

    Args:
        config_file (Path): Path to config file
        file_path_to_process (Path): Path to video stream
        output_path (Path): Path to output directory
    """
    params = utils.config(config_file, section="openface")

    max_retry = int(params["openface_max_retry"])
    retry_count = 1

    command_array = [
        "FeatureExtraction",
        "-f",
        file_path_to_process,
        "-out_dir",
        output_path,
    ]

    command_array = cli.singularity_run(config_file, command_array)

    # Fix OPENBLAS_NUM_THREADS to avoid error
    #
    # OpenBLAS : Program is Terminated. Because you tried to allocate too many memory regions.
    # This library was built to support a maximum of 128 threads - either rebuild OpenBLAS
    # with a larger NUM_THREADS value or set the environment variable OPENBLAS_NUM_THREADS to
    # a sufficiently small number. This error typically occurs when the software that relies on
    # OpenBLAS calls BLAS functions from many threads in parallel, or when your computer has more
    # cpu cores than what OpenBLAS was configured to handle.

    os.environ["OPENBLAS_NUM_THREADS"] = params["openblas_num_threads"]
    logger.debug(f"Setting OPENBLAS_NUM_THREADS to {params['openblas_num_threads']}")

    non_completed = True

    def _on_fail():
        nonlocal retry_count, non_completed
        non_completed = True
        logger.warning("[red]OpenFace failed.", extra={"markup": True})
        logger.info(
            f"[red]Clearing output path: {output_path}[/red]", extra={"markup": True}
        )
        cli.remove_directory(output_path)

        if retry_count >= max_retry:
            logger.error(
                f"[red]OpenFace failed after {max_retry} attempts.[/red]",
                extra={"markup": True},
            )
            logger.error(
                f"[red]File: {file_path_to_process}[/red]",
                extra={"markup": True},
            )
            logger.error(
                "Exiting with error code 1.",
                extra={"markup": True},
            )
            sys.exit(1)

        logger.warning(
            f"[yellow]Retrying OpenFace. Attempt {retry_count} of {max_retry}",
            extra={"markup": True},
        )
        retry_count += 1

    while retry_count < max_retry and non_completed:
        with utils.get_progress_bar() as progress:
            progress.add_task("[green]Running OpenFace...", total=None)

            non_completed = False
            cli.execute_commands(
                command_array=command_array,
                on_fail=_on_fail,
                logger=logger,
            )

    return


def log_openface(
    config_file: Path,
    video_stream_path: Path,
    interview_role: InterviewRole,
    video_path: Path,
    openface_path: Path,
    process_time: Optional[float] = None,
) -> None:
    """
    Log OpenFace to database

    Args:
        config_file (Path): Path to config file
        video_stream_path (Path): Path to video stream
        interview_role (InterviewRole): Interview role
        video_path (Path): Path to video
        openface_path (Path): Path to OpenFace output
        process_time (Optional[float], optional): Time it took to process the video.
            Defaults to None.
    """
    openface = Openface(
        vs_path=video_stream_path,
        ir_role=interview_role,
        video_path=video_path,
        of_processed_path=openface_path,
        of_process_time=process_time,
    )

    sql_query = openface.to_sql()

    logger.info("Logging OpenFace to DB", extra={"markup": True})
    db.execute_queries(config_file=config_file, queries=[sql_query])


def await_decrytion(config_file: Path, counter: int) -> None:
    """
    Request decryption and snooze if no more files to process

    Args:
        config_file (Path): Path to config file
        counter (int): Number of files processed
    """
    # Log if any files were processed
    if counter > 0:
        data.log(
            config_file=config_file,
            module_name=MODULE_NAME,
            message=f"Processed {counter} files.",
        )

    # Request decryption
    orchestrator.request_decrytion(config_file=config_file)
    # Snooze
    orchestrator.snooze(config_file=config_file)


if __name__ == "__main__":
    config_file = utils.get_config_file_path()
    utils.configure_logging(
        config_file=config_file, module_name=MODULE_NAME, logger=logger
    )

    console.rule(f"[bold red]{MODULE_NAME}")
    logger.info(f"Using config file: {config_file}")

    config_params = utils.config(config_file, section="general")
    study_id = config_params["study"]

    INSTANCE_NAME = utils.get_instance_name(
        module_name=INSTANCE_NAME, process_name=sys.argv[0]
    )

    COUNTER = 0
    SKIP_COUNTER = 0

    logger.info("[bold green]Starting OpenFace loop...", extra={"markup": True})

    STASH: Optional[Tuple[Path, InterviewRole, Path]] = None

    while True:
        # Get file to process
        if STASH is not None:
            file_to_process = STASH
            STASH = None
        else:
            file_to_process = get_file_to_process(
                config_file=config_file, study_id=study_id
            )

        if file_to_process is None:
            console.log("[bold green] No file to process.")
            await_decrytion(config_file=config_file, counter=COUNTER)
            COUNTER = 0
            continue

        video_stream_path, interview_role, video_path = file_to_process

        if not video_stream_path.exists():
            logger.error(f"Video stream path does not exist: {video_stream_path}")
            logger.error(f"video_path: {video_path}")
            sys.exit(1)

        openface_path = construct_output_path(
            config_file=config_file, video_path=video_stream_path
        )
        logger.debug(f"Output path: {openface_path}")
        # Check if another process is running with same files
        if cli.check_if_running(process_name=str(video_stream_path)):
            logger.warning(
                f"Another process is running with the same file: {video_stream_path}"
            )
            SKIP_COUNTER += 1
            if SKIP_COUNTER > orchestrator.get_max_instances(
                config_file=config_file,
                module_name=MODULE_NAME,
            ):
                console.log("[bold red]Max number of instances reached. Snoozing...")
                await_decrytion(config_file=config_file, counter=COUNTER)
                SKIP_COUNTER = 0
                COUNTER = 0
                continue
            file_to_process = get_file_to_process(
                config_file=config_file, study_id=study_id
            )
            continue
        else:
            SKIP_COUNTER = 0
            COUNTER += 1

        logger.info(
            f"Processing {video_stream_path} as {interview_role} from {video_path}"
        )

        # Run OpenFace
        with Timer() as timer:
            run_openface(
                config_file=config_file,
                file_path_to_process=video_stream_path,
                output_path=openface_path,
            )

        # Log to DB
        log_openface(
            config_file=config_file,
            video_stream_path=video_stream_path,
            interview_role=interview_role,
            video_path=video_path,
            openface_path=openface_path,
            process_time=timer.duration,
        )

        # Get other stream to process
        logger.info(f"Checking for other stream to process from {video_path}")
        STASH = get_other_stream_to_process(
            config_file=config_file, video_path=video_path
        )

        if file_to_process is None:
            console.log("[bold green] No other stream to process.")
            continue
