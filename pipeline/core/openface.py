"""
Helper functions for running OpenFace
"""

import logging
import sys
from pathlib import Path
from typing import Optional, Tuple, List
import multiprocessing
import tempfile

from pipeline import core, orchestrator
from pipeline.helpers import cli, db, dpdash, utils, image, ffmpeg
from pipeline.models.interview_roles import InterviewRole
from pipeline.models.openface import Openface

logger = logging.getLogger(__name__)


def get_file_to_process(
    config_file: Path, study_id: str
) -> Optional[Tuple[Path, InterviewRole, Path, str]]:
    """
    Fetch a file to process from the database.

    - Fetches a file that has not been processed yet and is part of the study.
        - Must have completed split-streams process

    Args:
        config_file (Path): Path to config file
        study_id (str): Study ID

    Returns:
        Optional[Tuple[Path, InterviewRole, Path, str]]: Tuple of video stream path,
            interview role, video path and interview name
    """
    sql_query = f"""
        SELECT vs.vs_path, vs.ir_role, vs.video_path, if.interview_name
        FROM video_streams AS vs
        INNER JOIN (
            SELECT decrypted_files.destination_path, interview_files.interview_file_tags, interviews.interview_name
            FROM interview_files
            JOIN decrypted_files
                ON interview_files.interview_file = decrypted_files.source_path
            join interviews using (interview_path)
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
    interview_name = result_df.iloc[0]["interview_name"]

    return video_stream_path, interview_role, video_path, interview_name


def get_other_stream_to_process(
    config_file: Path, video_path: Path
) -> Optional[Tuple[Path, InterviewRole, Path, str]]:
    """
    Fetch a file to process from the database.

    - Fetches a file that has not been processed yet and
        is part of the study.
    - Fetches video stream that is part of the same video as the
        video stream that was just processed.

    Args:
        config_file (Path): Path to config file

    Returns:
        Optional[Tuple[Path, InterviewRole, Path, str]]: Tuple of video stream path,
            interview role, video path and interview name
    """
    sql_query = f"""
        SELECT vs.vs_path, vs.ir_role, vs.video_path, if.interview_name
        FROM video_streams AS vs
        INNER JOIN (
            SELECT decrypted_files.destination_path, interview_files.interview_file_tags, interviews.interview_name
            FROM interview_files
            JOIN decrypted_files
                ON interview_files.interview_file = decrypted_files.source_path
            join interviews using (interview_path)
        ) AS if
        ON vs.video_path = if.destination_path
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
    interview_name = result_df.iloc[0]["interview_name"]

    return video_stream_path, interview_role, video_path, interview_name


def construct_output_path(config_file: Path, video_path: Path) -> Path:
    """
    Construct output path for OpenFace output

    Args:
        config_file (Path): Path to config file
        video_path (Path): Path to video
    """
    data_root = orchestrator.get_data_root(config_file=config_file)

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
        data_type,  # type: ignore
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

    openblas_num_threads = params.get("openblas_num_threads", "4")
    cli.set_environment_variable("OPENBLAS_NUM_THREADS", openblas_num_threads)

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
            cli.execute_commands(command_array=command_array, on_fail=_on_fail)

    return


def pad_image_process(params: Tuple[Path, Path, int]) -> None:
    """
    Pad image process

    Args:
        params (Tuple[Path, Path, Path]): Tuple of input image, output image, and padding
            size

    Returns:
        None
    """
    input_image, output_image, padding_size = params

    image.pad_image(
        source_image=input_image,
        dest_image=output_image,
        padding=padding_size,
    )

    return


def run_openface_overlay(
    config_file: Path,
    openface_path: Path,
    output_video_path: Path,
    temp_dir_prefix: str = "openface_overlay_",
) -> None:
    """
    Runs OpenFace on face_aligned frames generated by previous OpenFace run.

    Does:
    - Pads face_aligned frames with 100px on all sides
    - Compiles frames into video
    - Run OpenFace on compiled video
    - Crops out 75px out of each side of the video
    - Return the OpenFace video

    Args:
        config_file (Path): Path to config file
        openface_path (Path): Path to OpenFace output
        output_video_path (Path): Path to output video

    Returns:
        None
    """

    aligned_images_path = next(openface_path.glob("*aligned"), None)

    if aligned_images_path is None:
        logger.error(f"No aligned directory found in {openface_path}")
        raise ValueError(f"No aligned directory found in {openface_path}")

    image_files = list(aligned_images_path.glob("*.bmp"))
    logger.info("Padding face_aligned frames", extra={"markup": True})

    params: List[Tuple[Path, Path, int]] = []

    with tempfile.TemporaryDirectory(prefix=temp_dir_prefix) as temp_dir:
        temp_dir_path = Path(temp_dir)
        for img in image_files:
            output_img = temp_dir_path / img.name
            params.append((img, output_img, 100))

        with utils.get_progress_bar() as progress:
            task = progress.add_task(
                "[green]Padding face_aligned frames...", total=len(params)
            )

            num_processes = int(multiprocessing.cpu_count() / 2)
            logger.debug(f"Using {num_processes} processes to pad images")

            with multiprocessing.Pool(processes=num_processes) as pool:
                for _ in pool.imap_unordered(pad_image_process, params):
                    progress.update(task, advance=1)

        logger.info("Compiling frames into video", extra={"markup": True})
        video_path = temp_dir_path / "video.mp4"

        ffmpeg.images_to_vid(
            image_dir=temp_dir_path,
            output_file=video_path,
        )

        logger.info("Running OpenFace on compiled video", extra={"markup": True})
        openface_path = temp_dir_path / "openface"

        run_openface(
            config_file=config_file,
            file_path_to_process=video_path,
            output_path=openface_path,
        )

        openface_video = next(openface_path.glob("*.avi"), None)
        if openface_video is None:
            raise ValueError(f"No video found in {openface_path}")

        logger.info(
            "Cropping out 75px out of each side of the video", extra={"markup": True}
        )
        ffmpeg.crop_video(
            source=openface_video,
            target=output_video_path,
            crop_params="162:162:75:75",
        )

    return


def clean_up_after_openface(openface_path: Path) -> None:
    """
    Clean up after OpenFace

    Removes:
    - face_aligned frames
    - hog files
    """

    logger.info("Cleaning up after OpenFace", extra={"markup": True})

    aligned_images_path = next(openface_path.glob("*aligned"), None)

    if aligned_images_path is not None:
        cli.remove_directory(aligned_images_path)

    hog_files = list(openface_path.glob("*.hog"))

    for hog_file in hog_files:
        hog_file.unlink()

    return


def log_openface(
    config_file: Path,
    video_stream_path: Path,
    interview_role: InterviewRole,
    video_path: Path,
    openface_path: Path,
    openface_process_time: Optional[float] = None,
    overlay_process_time: Optional[float] = None,
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
        of_process_time=openface_process_time,
        of_overlay_provess_time=overlay_process_time,
    )

    sql_query = openface.to_sql()

    logger.info("Logging OpenFace to DB", extra={"markup": True})
    db.execute_queries(config_file=config_file, queries=[sql_query])

    return


def await_decrytion(config_file: Path, module_name: str, counter: int) -> None:
    """
    Request decryption and snooze if no more files to process

    Args:
        config_file (Path): Path to config file
        module_name (str): Name of module that ran OpenFace
        counter (int): Number of files processed
    """
    # Log if any files were processed
    if counter > 0:
        core.log(
            config_file=config_file,
            module_name=module_name,
            message=f"Processed {counter} files.",
        )

    # Request decryption
    orchestrator.request_decrytion(config_file=config_file)
    # Snooze
    orchestrator.snooze(config_file=config_file)

    return
