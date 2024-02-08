"""
Quality Control Section for Apperance and Behavior Section
"""

import sys
import tempfile
from datetime import timedelta
from pathlib import Path
from typing import Optional

from reportlab.pdfgen import canvas

from pipeline import data
from pipeline.helpers import dpdash, image, pdf, utils
from pipeline.models.interview_roles import InterviewRole
from pipeline.models.lite.frame_request import FrameRequest
from pipeline.models.lite.openface_qc_metrics import OpenFaceQcMetrics
from pipeline.models.lite.video_metadata import VideoMetadata

console = utils.get_console()


def draw_sample_image(
    canvas: canvas.Canvas,
    image_path: Path,
    role: InterviewRole,
):
    """
    Draw the sample image for the video section. Large image on the right side.

    Args:
        canvas (canvas.Canvas): The canvas to draw on.
        image_path (Path): The path to the image.
        role (InterviewRole): The role for whom the image is being drawn

    Raises:
        ValueError: If the role is invalid.

    Returns:
        None
    """
    sample_height = 86
    sample_width = 152
    # sample_height = 85.33
    # sample_width = 48
    sample_left = 605
    pt_sample_bottom = 184.01
    int_sample_bottom = 444.46

    match role:
        case InterviewRole.SUBJECT:
            y = pt_sample_bottom
        case InterviewRole.INTERVIEWER:
            y = int_sample_bottom
        case _:
            raise ValueError(f"Invalid role: {role}")

    x = sample_left + 32
    y = y + 3

    scale_factor = 0.6

    pdf.draw_image(
        canvas,
        image_path,
        x,
        y,
        sample_width * scale_factor,
        sample_height * scale_factor,
    )


def construct_sample_image_by_role(
    canvas: canvas.Canvas,
    interview_name: str,
    role: InterviewRole,
    config_file: Path,
    start_time: timedelta = timedelta(hours=0, minutes=0, seconds=0),
    end_time: timedelta = timedelta(hours=0, minutes=30, seconds=0),
):
    """
    Fetches the sample image for the video section and draws it on the canvas. Selects
    a frame from the middle of the video.

    Args:
        canvas (canvas.Canvas): The canvas to draw on.
        interview_name (str): The name of the interview.
        role (InterviewRole): The role for whom the image is being drawn
        config_file (Path): The path to the configuration file.
        start_time (timedelta): The start time of the video.
        end_time (timedelta): The end time of the video.

    Raises:
        FileNotFoundError: If the OpenFace video is not found.
        FileNotFoundError: If the frame number is not found.

    Returns:
        None
    """
    of_path = data.get_openface_path(
        interview_name=interview_name, role=role, config_file=config_file
    )

    if of_path is None:
        raise FileNotFoundError(f"OpenFace path not found for {interview_name} {role}")

    of_video_path = of_path.glob("*.avi")
    of_video = next(of_video_path, None)

    if of_video is None:
        raise FileNotFoundError(f"OpenFace video not found for {interview_name} {role}")

    # Get a frame at the middle of the video
    # Get frame number from openface_features table

    frame_number: Optional[int] = None
    frame_request: Optional[FrameRequest] = None
    retry_time = timedelta(minutes=5)
    retry_counter = 0
    max_retires = 3
    dpdash_dict = dpdash.parse_dpdash_name(interview_name)
    study_id = dpdash_dict["study"]
    subject_id = dpdash_dict["subject"]

    while frame_number is None and retry_counter < max_retires:
        if retry_counter > 0:
            console.log(
                f"[red]Retrying frame request for {interview_name} {role} \
after {retry_counter} attempts"
            )
            console.log(f"Failed FrameRequest: {frame_request}")
            start_time = start_time - retry_time
            start_time = max(start_time, timedelta(hours=0, minutes=0, seconds=0))

            end_time = end_time + retry_time

        retry_counter = retry_counter + 1
        frame_request = FrameRequest(
            interview_name=interview_name,
            study_id=study_id,  # type: ignore
            subject_id=subject_id,  # type: ignore
            role=role,
            start_time=start_time,
            end_time=end_time,
        )

        frame_number = FrameRequest.get_frame_number(
            request=frame_request, config_file=config_file
        )

    if frame_number is None:
        console.log(
            f"[bold red]Failed to get frame number for {interview_name} {role} \
after {max_retires} attempts"
        )
        return

    with tempfile.NamedTemporaryFile(suffix=".png") as sample_frame:
        image.get_frame_by_number(
            video_path=of_video,
            frame_number=frame_number,
            dest_image=Path(sample_frame.name),
        )

        with tempfile.NamedTemporaryFile(suffix=".png") as deidentified_image:
            image.draw_bars_over_image(
                source_image=Path(sample_frame.name),
                dest_image=Path(deidentified_image.name),
                start_h=0.95,
                end_h=1,
                bar_color=(255, 255, 255),
            )
            draw_sample_image(
                canvas=canvas, image_path=Path(deidentified_image.name), role=role
            )


def construct_openface_metadata_box_by_role(
    canvas: canvas.Canvas,
    role: InterviewRole,
    interview_name: str,
    config_file: Path,
) -> None:
    """
    Constructs the OpenFace metadata box for the video section.

    Args:
        canvas (canvas.Canvas): The canvas to draw on.
        role (InterviewRole): The role for whom the metadata box is being placed.
        interview_name (str): The name of the interview.
        config_file (Path): The path to the configuration file.

    Raises:
        ValueError: If the role is invalid.

    Returns:
        None
    """
    openface_info = ["OpenFace 2.2.0"]

    # Get current python version
    python_version = sys.version.split(" ", maxsplit=1)[0]

    openface_info.append(f"Python {python_version}")

    vid_metadata: VideoMetadata = VideoMetadata.get(
        interview_name=interview_name, role=role, config_file=config_file
    )

    resolution_text = (
        f"Resolution: {vid_metadata.video_width}x{vid_metadata.video_height}"
    )

    # Drow Box
    vid_box_left = 733.92
    vid_box_width = 70.854
    vid_box_height = 40.269

    vid_box_pt_bottom = 173.7
    vid_box_int_bottom = 434.47

    match role:
        case InterviewRole.SUBJECT:
            vid_box_y = vid_box_pt_bottom
        case InterviewRole.INTERVIEWER:
            vid_box_y = vid_box_int_bottom
        case _:
            raise ValueError(f"Invalid role: {role}")

    pdf.draw_colored_rect(
        canvas=canvas,
        x=vid_box_left,
        y=vid_box_y,
        width=vid_box_width,
        height=vid_box_height,
        color=(0.7, 0.7, 0.7),  # type: ignore
        fill=False,
        stroke=True,
        line_width=1,
    )

    # Draw text
    pdf.draw_text(
        canvas=canvas,
        text=resolution_text,
        x=vid_box_left + 4,
        y=vid_box_y - 30,
        size=5,
        font="Helvetica",
    )

    face_text_left = 750
    face_text_bot_pt = 158
    face_text_bot_int = 418

    code_text_small_gap = 6

    match role:
        case InterviewRole.SUBJECT:
            face_text_bot = face_text_bot_pt
        case InterviewRole.INTERVIEWER:
            face_text_bot = face_text_bot_int
        case _:
            raise ValueError(f"Invalid role: {role}")

    for text in openface_info:
        pdf.draw_text(
            canvas,
            text,
            face_text_left,
            face_text_bot,
            5,
            "Helvetica",
        )
        face_text_bot = face_text_bot + code_text_small_gap


def draw_qc_metrics_by_role(
    canvas: canvas.Canvas,
    role: InterviewRole,
    interview_name: str,
    x: float,
    config_file: Path,
) -> None:
    """
    Draw the QC metrics for the video section.

    QC Metrics include:
    - Percentage of successful frames
    - Confidence mean of successful frames

    The text is colored based on the value of the metric.

    Args:
        canvas (canvas.Canvas): The canvas to draw on.
        role (InterviewRole): The role for whom the QC metrics are being drawn.
        interview_name (str): The name of the interview.
        x (float): The x position of the text.
        config_file (Path): The path to the configuration file.

    Raises:
        ValueError: If the role is invalid.

    Returns:
        None
    """
    qc_metrics = OpenFaceQcMetrics.get(
        interview_name=interview_name, role=role, config_file=config_file
    )

    pt_sample_qc_text_bot = 195
    int_sample_qc_text_bot = 455.5

    green_color = (0, 0.4, 0)
    orange_color = (1, 0.5, 0)
    red_color = (1, 0, 0)

    if qc_metrics.successful_frames_percentage > 0.85:
        frames_color = green_color
    elif qc_metrics.successful_frames_percentage > 0.5:
        frames_color = orange_color
    else:
        frames_color = red_color

    if qc_metrics.successful_frames_confidence_mean > 0.95:
        confidence_color = green_color
    elif qc_metrics.successful_frames_confidence_mean > 0.8:
        confidence_color = orange_color
    else:
        confidence_color = red_color

    match role:
        case InterviewRole.SUBJECT:
            y = pt_sample_qc_text_bot
        case InterviewRole.INTERVIEWER:
            y = int_sample_qc_text_bot
        case _:
            raise ValueError(f"Invalid role: {role}")

    frames_qc_text = (
        f"Faces detected in {qc_metrics.successful_frames_percentage:.2f}% of frames"
    )
    confidence_qc_text = (
        f"{qc_metrics.successful_frames_confidence_mean * 100:.2f}% OF confidence mean"
    )

    pdf.draw_text(
        canvas=canvas,
        text=frames_qc_text,
        x=x + 20,
        y=y,
        size=5,
        font="Helvetica-Bold",
        color=frames_color,  # type: ignore
    )

    pdf.draw_text(
        canvas=canvas,
        text=confidence_qc_text,
        x=x + 20,
        y=y + 8,
        size=5,
        font="Helvetica-Bold",
        color=confidence_color,  # type: ignore
    )
