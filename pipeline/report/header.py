"""
Functions to construct the header of the report.
"""

from datetime import datetime
from pathlib import Path

from reportlab.pdfgen import canvas

from pipeline.helpers import pdf
from pipeline.models.lite.interview_metadata import InterviewMetadata


def construct_header(
    assets_path: Path,
    canvas: canvas.Canvas,
    output_path: Path,
    interview_metadata: InterviewMetadata,
):
    """
    Constructs the header of the report.

    Args:
        assets_path (Path): The path to the assets directory.
        canvas (reportlab.pdfgen.canvas.Canvas): The canvas to draw on.
        output_path (Path): The path to the output file.
        interview_metadata (InterviewMetadata): The metadata for the interview.

    Returns:
        None
    """
    mcl_path = assets_path / "svgs" / "mcl_logo.svg"
    itp_path = assets_path / "svgs" / "itp_logo.svg"

    mclean_logo_left = 697.46
    itp_logo_left = 751.21
    logo_bottom = 67
    logo_width = 45.16
    logo_height = 54.268

    pdf.draw_svg(
        canvas, mcl_path, mclean_logo_left, logo_bottom, logo_width, logo_height
    )
    pdf.draw_svg(canvas, itp_path, itp_logo_left, logo_bottom, logo_width, logo_height)

    title_left = 20.32
    title_bottom = 35.24

    path_left = 20.38
    path_bottom = 73.69
    time_bottom = 58.03

    interview_type = interview_metadata.interview_type.capitalize()

    title = f"{interview_type} Interview Report"
    time_generated = str(datetime.now()).split(".", maxsplit=1)[0]
    date = time_generated.split(" ")[0]
    time = time_generated.split(" ")[1]
    time_text = f"Report Generated: {date} at {time}"

    pdf.draw_text(
        canvas, title, title_left, title_bottom, 20, "Helvetica-Bold", center=False
    )
    pdf.draw_text(
        canvas, time_text, path_left, time_bottom, 8, "Helvetica", center=False
    )
    pdf.draw_text(
        canvas,
        str(output_path),
        path_left,
        path_bottom,
        4,
        "Helvetica-Oblique",
        center=False,
    )

    params_col1 = ["Study", "SubID", "Visit"]
    params_col2 = ["StudyDay", "Time", "Length"]

    params_col1_left = 374.77
    params_col2_left = 543.47
    params_col_bottoms = [33.45, 49.45, 65.45]

    params_answer_col1_right = 526.66
    params_answer_col2_right = 660.93

    metadata_col1 = interview_metadata.get_params_col1()
    metadata_col2 = interview_metadata.get_params_col2()

    metadata_col1_left = pdf.compute_x_right_align_list(
        metadata_col1, "Helvetica-Bold", 12, params_answer_col1_right
    )
    metadata_col2_left = pdf.compute_x_right_align_list(
        metadata_col2, "Helvetica-Bold", 12, params_answer_col2_right
    )

    for text, bot in zip(params_col1, params_col_bottoms):
        pdf.draw_text(
            canvas, text, params_col1_left, bot, 12, "Helvetica", center=False
        )

    for text, bot in zip(params_col2, params_col_bottoms):
        pdf.draw_text(
            canvas, text, params_col2_left, bot, 12, "Helvetica", center=False
        )

    for text, left, bot in zip(metadata_col1, metadata_col1_left, params_col_bottoms):
        pdf.draw_text(canvas, text, left, bot, 12, "Helvetica-Bold", center=False)

    for text, left, bot in zip(metadata_col2, metadata_col2_left, params_col_bottoms):
        pdf.draw_text(canvas, text, left, bot, 12, "Helvetica-Bold", center=False)
