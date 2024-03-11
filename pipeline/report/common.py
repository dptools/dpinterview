"""
Common functions for report generation.
"""

from pathlib import Path
from typing import List

from reportlab.pdfgen import canvas

from pipeline import core
from pipeline.helpers import pdf
from pipeline.models.lite.interview_metadata import InterviewMetadata
from pipeline.models.lite.ticks_config import TicksConfig


def draw_heatmap_legend(
    assets_path: Path, canvas: canvas.Canvas, data_type: str
) -> None:
    """
    Draws the heatmap legend on the canvas.

    Args:
        assets_path (Path): The path to the assets directory.
        canvas (canvas.Canvas): The canvas to draw on.
        data_type (str): The type of data, either "video" or "audio".

    Returns:
        None
    """
    heatmap_legend_path = assets_path / "svgs" / "heatmap_legend.svg"

    heatmap_legend_left = 185.08
    heatmap_legend_bottom_vid = 377.64
    heatmap_legend_width = 333.871
    heatmap_legend_height = 9.902
    heatmap_legend_bottom_aud = 804.67

    if data_type == "video":
        pdf.draw_svg(
            canvas,
            heatmap_legend_path,
            heatmap_legend_left,
            heatmap_legend_bottom_vid,
            heatmap_legend_width,
            heatmap_legend_height,
        )
    elif data_type == "audio":
        pdf.draw_svg(
            canvas,
            heatmap_legend_path,
            heatmap_legend_left,
            heatmap_legend_bottom_aud,
            heatmap_legend_width,
            heatmap_legend_height,
        )


def draw_corr_matrix_legend(assets_path: Path, canvas: canvas.Canvas, data_type: str):
    """
    Draws the correlation matrix legend on the canvas.

    Args:
        assets_path (Path): The path to the assets directory.
        canvas (canvas.Canvas): The canvas to draw on.
        data_type (str): The type of data, either "video" or "audio".

    Returns:
        None
    """
    corr_matrix_legend_path = assets_path / "svgs" / "corr_matrix_legend.svg"

    corr_matrix_legend_left_vid = 675.65
    corr_matrix_legend_left_aud = 687.0
    corr_matrix_legend_bottom_vid = 374.66
    corr_matrix_legend_width = 105.123
    corr_matrix_legend_height = 9.475
    corr_matrix_legend_bottom_aud = 809.5

    if data_type == "video":
        pdf.draw_svg(
            canvas,
            corr_matrix_legend_path,
            corr_matrix_legend_left_vid,
            corr_matrix_legend_bottom_vid,
            corr_matrix_legend_width,
            corr_matrix_legend_height,
        )
    elif data_type == "audio":
        pdf.draw_svg(
            canvas,
            corr_matrix_legend_path,
            corr_matrix_legend_left_aud,
            corr_matrix_legend_bottom_aud,
            corr_matrix_legend_width,
            corr_matrix_legend_height,
        )


def construct_heatmap_ticks(
    canvas: canvas.Canvas,
    y: float,
    ticks_config: TicksConfig,
    min_labels: List[str],
    minutes_start_x: float,
    tick_left: float,
):
    """
    Constructs the heatmap ticks on the canvas.

    Args:
        canvas (canvas.Canvas): The canvas to draw on.
        y (float): The y-coordinate of the ticks.
        ticks_config (TicksConfig): The ticks configuration.
        min_labels (List[str]): The list of minute labels.
        minutes_start_x (float): The x-coordinate to start drawing the minute labels.
        tick_left (float): The x-coordinate to start drawing the ticks.

    Returns:
        None
    """
    minutes_start_y_int_vid = ticks_config.min_label_offset_ticks_y + y

    tick_start_y = ticks_config.ticks_offset_y + y

    # Interviewer heatmap time
    cur_left = minutes_start_x
    for time_l in min_labels:
        pdf.draw_text(canvas, time_l, cur_left, minutes_start_y_int_vid, 4, "Helvetica")
        cur_left = cur_left + ticks_config.ticks_large_spacing

    # ticks
    cur_left = tick_left
    for _ in range(31):
        pdf.draw_line(
            canvas,
            cur_left,
            tick_start_y,
            cur_left,
            tick_start_y + ticks_config.tick_height,
            0.25,
        )
        cur_left = cur_left + ticks_config.ticks_small_spacing


def print_visit_and_participant_metadata(
    canvas: canvas.Canvas,
    interview_metadata: InterviewMetadata,
    config_file: Path,
    data_type: str,
) -> None:
    """
    Prints the visit and participant metadata on the canvas.

    Args:
        canvas (canvas.Canvas): The canvas to draw on.
        interview_metadata (InterviewMetadata): The interview metadata.
        config_file (Path): The path to the configuration file.
        data_type (str): The type of data, either "video" or "audio".

    Returns:
        None
    """
    study_id = interview_metadata.study

    n_of_left = 75
    heatmap_legend_bottom_vid = 377.64
    heatmap_legend_bottom_aud = 804.67
    n_of_bot_vid = heatmap_legend_bottom_vid - 4
    n_of_bot_aud = heatmap_legend_bottom_aud - 4

    study_visits_count = core.get_study_visits_count(
        study_id=study_id, config_file=config_file
    )
    study_subjects_count = core.get_study_subjects_count(
        study_id=study_id, config_file=config_file
    )
    n_of_text = (
        "n = "
        + str(study_visits_count)
        + " Visits / "
        + str(study_subjects_count)
        + " Participants"
    )

    match data_type:
        case "video":
            pdf.draw_text(
                canvas=canvas,
                text=n_of_text,
                x=n_of_left,
                y=n_of_bot_vid,
                size=5,
                font="Helvetica",
            )
        case "audio":
            pdf.draw_text(
                canvas=canvas,
                text=n_of_text,
                x=n_of_left,
                y=n_of_bot_aud,
                size=5,
                font="Helvetica",
            )


def print_page_numbers(
    canvas: canvas.Canvas,
    current_page: int,
    total_pages: int,
) -> None:
    """
    Draws the page numbers on the canvas.

    Args:
        canvas (canvas.Canvas): The canvas to draw on.
        current_page (int): The current page number.
        total_pages (int): The total number of pages.

    Returns:
        None
    """
    page_marker_left = 765.58
    page_marker_bot = 1050.63

    page_marker_text = f"Front Page ({current_page}/{total_pages})"

    pdf.draw_text(
        canvas=canvas,
        text=page_marker_text,
        x=page_marker_left,
        y=page_marker_bot,
        size=5,
        font="Helvetica-Bold",
    )
