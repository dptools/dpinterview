"""
Constructs Correlation Matrix and other metrics for
Apperance and Movement Section
"""

from pathlib import Path
from typing import List, Tuple

import numpy as np
import pandas as pd
from reportlab.pdfgen import canvas

from pipeline import data
from pipeline.helpers import dpdash, pdf, utils
from pipeline.models.interview_roles import InterviewRole
from pipeline.models.lite.interview_metadata import InterviewMetadata

console = utils.get_console()


def construct_corr_matrix_by_role(
    canvas: canvas.Canvas,
    role: InterviewRole,
    interview_metadata: InterviewMetadata,
    corr_matrix_path: Path,
) -> None:
    """
    Places the correlation matrix on the canvas based on the role.

    Args:
        canvas (canvas.Canvas): The canvas to draw on.
        role (InterviewRole): The role for whom the correlation matrix is being placed.
        interview_metadata (InterviewMetadata): The interview metadata.
        corr_matrix_path (Path): The path to the correlation matrix.

    Raises:
        ValueError: If the role is invalid.

    Returns:
        None
    """
    corr_matrix_vid_left = 637.63
    corr_matrix_vid_width = 138.129
    corr_matrix_vid_height = 127
    corr_matrix_vid_pt_bottom = 351.5
    corr_matrix_vid_int_bottom = 605.24

    pt_vid_corr_top_rt_header = "Participant FAU Correlation (All Time)"
    int_vid_corr_top_rt_header = "Overall FAU Correlation (All Time)"

    visit = interview_metadata.visit

    pt_vid_corr_bot_lf_header = f"Visit {str(visit)} Participant FAU Correlation"
    int_vid_corr_bot_lf_header = f"Visit {str(visit)} Interviewer FAU Correlation"

    pt_vid_corr_top_rt_text_bot = corr_matrix_vid_pt_bottom - corr_matrix_vid_height - 2
    pt_vid_corr_bot_lf_text_left = corr_matrix_vid_left
    pt_vid_corr_bot_lf_text_bot = corr_matrix_vid_pt_bottom + 7
    int_vid_corr_top_rt_text_left = (
        corr_matrix_vid_left
        + corr_matrix_vid_width
        - pdf.stringWidth(int_vid_corr_top_rt_header, "Helvetica", 5) / float(pdf.cw)
    )
    pt_vid_corr_top_rt_text_left = (
        corr_matrix_vid_left
        + corr_matrix_vid_width
        - pdf.stringWidth(pt_vid_corr_top_rt_header, "Helvetica", 5) / float(pdf.cw)
    )
    int_vid_corr_top_rt_text_bot = (
        corr_matrix_vid_int_bottom - corr_matrix_vid_height - 2
    )
    int_vid_corr_bot_lf_text_left = corr_matrix_vid_left
    int_vid_corr_bot_lf_text_bot = corr_matrix_vid_int_bottom + 7

    match role:
        case InterviewRole.SUBJECT:
            corr_matrix_y = corr_matrix_vid_pt_bottom
            top_rt_header = pt_vid_corr_top_rt_header
            bot_lf_header = pt_vid_corr_bot_lf_header
            top_rt_text_left = pt_vid_corr_top_rt_text_left
            top_rt_text_bot = pt_vid_corr_top_rt_text_bot
            bot_lf_text_left = pt_vid_corr_bot_lf_text_left
            bot_lf_text_bot = pt_vid_corr_bot_lf_text_bot
        case InterviewRole.INTERVIEWER:
            corr_matrix_y = corr_matrix_vid_int_bottom
            top_rt_header = int_vid_corr_top_rt_header
            bot_lf_header = int_vid_corr_bot_lf_header
            top_rt_text_left = int_vid_corr_top_rt_text_left
            top_rt_text_bot = int_vid_corr_top_rt_text_bot
            bot_lf_text_left = int_vid_corr_bot_lf_text_left
            bot_lf_text_bot = int_vid_corr_bot_lf_text_bot
        case _:
            raise ValueError(f"Invalid role: {role}")

    pdf.draw_text(
        canvas,
        top_rt_header,
        top_rt_text_left,
        top_rt_text_bot,
        5,
        "Helvetica",
    )
    pdf.draw_text(
        canvas,
        bot_lf_header,
        bot_lf_text_left,
        bot_lf_text_bot,
        5,
        "Helvetica",
    )

    pdf.draw_image(
        canvas,
        corr_matrix_path,
        corr_matrix_vid_left,
        corr_matrix_y,
        corr_matrix_vid_width,
        corr_matrix_vid_height,
    )


def contruct_dendrogram_by_role(
    canvas: canvas.Canvas,
    role: InterviewRole,
    assets_path: Path,
) -> None:
    """
    Places the dendrogram on the canvas based on the role.

    Looks for the dendrogram at the assets path:
    assets/protected/dendrogram_vid.png

    Args:
        canvas (canvas.Canvas): The canvas to draw on.
        role (InterviewRole): The role for whom the dendrogram is being placed.
        assets_path (Path): The path to the assets directory.

    Raises:
        ValueError: If the role is invalid.
        FileNotFoundError: If the dendrogram is not found.

    Returns:
        None
    """
    dendrogram_rot = 270
    dendrogram_vid_left = 771.8
    dendrogram_vid_width = 25.7
    dendrogram_vid_height = 124.605 + 8
    dendrogram_pt_vid_bottom = 351.06 + 4
    dendrogram_int_vid_bottom = 604.53 + 4

    dendro_vid_path = assets_path / "protected" / "dendrogram_vid.png"

    if not dendro_vid_path.exists():
        raise FileNotFoundError(f"Dendrogram not found at {dendro_vid_path}")

    match role:
        case InterviewRole.SUBJECT:
            img_y = dendrogram_pt_vid_bottom
            text_y = dendrogram_pt_vid_bottom - dendrogram_vid_height / 2.0
        case InterviewRole.INTERVIEWER:
            img_y = dendrogram_int_vid_bottom
            text_y = dendrogram_int_vid_bottom - dendrogram_vid_height / 2.0
        case _:
            raise ValueError(f"Invalid role: {role}")

    pdf.draw_image(
        canvas=canvas,
        image_path=dendro_vid_path,
        x=dendrogram_vid_left,
        y=img_y,
        width=dendrogram_vid_width,
        height=dendrogram_vid_height,
    )

    vid_dend_header = "Participant Group FAU Clusters"

    pdf.draw_text_vertical_centered_270(
        canvas=canvas,
        text=vid_dend_header,
        x=dendrogram_vid_left + dendrogram_vid_width + 3,
        y=text_y,
        angle=dendrogram_rot,
        size=5,
        font="Helvetica",
    )


def draw_pose_table_header(canvas: canvas.Canvas, role: InterviewRole) -> None:
    """
    Writes the header for the pose mean tables.

    Args:
        canvas (canvas.Canvas): The canvas to draw on.
        role (InterviewRole): The role for whom the header is being drawn.

    Raises:
        ValueError: If the role is invalid.

    Returns:
        None
    """
    table_text_left_vid = 600

    table_text_left_vid2 = 616.4

    table_cell_int_fau_left = 607
    table_text_left_vid_int = table_cell_int_fau_left + 4

    table_cell_height_pose = 7.5
    table_cell_int_pose_bot_start = 420

    pose_table_text_left = table_text_left_vid - 1
    pose_table_text_left2 = table_text_left_vid2 + 1
    pose_table_text_bot = 157.5
    pose_table_text_left_int = table_text_left_vid_int - 1.5
    pose_table_text_bot_int = table_cell_int_pose_bot_start - table_cell_height_pose - 1

    match role:
        case InterviewRole.SUBJECT:
            pdf.draw_text(
                canvas=canvas,
                text="GpMean",
                x=pose_table_text_left,
                y=pose_table_text_bot,
                size=3,
                font="Helvetica",
            )

            pdf.draw_text(
                canvas=canvas,
                text="PtMean",
                x=pose_table_text_left2,
                y=pose_table_text_bot,
                size=3,
                font="Helvetica",
            )
        case InterviewRole.INTERVIEWER:
            pdf.draw_text(
                canvas=canvas,
                text="GpMean",
                x=pose_table_text_left_int,
                y=pose_table_text_bot_int,
                size=3,
                font="Helvetica",
            )
        case _:
            raise ValueError(f"Invalid role: {role}")


def construct_pose_mean_tables_by_role(
    canvas: canvas.Canvas,
    role: InterviewRole,
    interview_name: str,
    config_file: Path,
    data_path: Path,
    pose_cols: List[str],
    gaze_cols: List[str],
) -> None:
    """
    Constructs the pose mean tables for the video section.

    Reads the pose mean tables from the metrics cache and compares them to the session pose means.
    The cache is expected to be at the data path:
    data/metrics_cache_{role}.csv

    The relative means are calculated as:
    gp_relative_means = (cache_means - session_means) * -1

    The relative means are then colore coded and placed in the table.

    Args:
        canvas (canvas.Canvas): The canvas to draw on.
        role (InterviewRole): The role for whom the pose mean tables are being placed.
        interview_name (str): The name of the interview.
        config_file (Path): The path to the configuration file.
        data_path (Path): The path to the data directory.
        pose_cols (List[str]): The list of pose columns.
        gaze_cols (List[str]): The list of gaze columns.

    Raises:
        ValueError: If the role is invalid.
        FileNotFoundError: If the metrics cache is not found.

    Returns:
        None
    """

    def process_number(number: float) -> str:
        if number < 0:
            num_str = f"{number}"
        else:
            num_str = f"+{number}"

        num_str = num_str[:5]

        return num_str

    required_cols = pose_cols + gaze_cols

    table_cell_pose_left = 597.02

    table_cell_width = 18.05
    table_cell_height_pose = 7.5

    table_cell_pose_bot_start = 165.5
    table_cell_int_pose_bot_start = 420

    table_cell_int_pose_left = 607

    draw_pose_table_header(canvas, role)
    means_and_std_path = data_path / f"metrics_cache_{role}.csv"

    if not means_and_std_path.exists():
        raise FileNotFoundError(f"Metrics cache not found at {means_and_std_path}")

    means_and_std = pd.read_csv(means_and_std_path)

    # Keep only POSE columns
    means_and_std = means_and_std[required_cols]

    dpdash_dict = dpdash.parse_dpdash_name(interview_name)
    study_id = dpdash_dict["study"]
    subject_id = dpdash_dict["subject"]

    session_of_pose_features = data.fetch_openface_features(
        interview_name=interview_name,
        subject_id=subject_id,
        study_id=study_id,
        role=role,
        cols=required_cols,
        config_file=config_file,
    )
    session_pose_means = session_of_pose_features.mean(axis=0)

    gp_relative_means = (means_and_std.iloc[0] - session_pose_means) * -1

    match role:
        case InterviewRole.SUBJECT:
            subject_of_pose_features = data.fetch_openface_subject_distribution(
                subject_id=subject_id,
                cols=required_cols,
                config_file=config_file,
            )
            subject_pose_means = subject_of_pose_features.mean(axis=0)

            pt_relative_means = (subject_pose_means - session_pose_means) * -1

            y = table_cell_pose_bot_start
            for feature in required_cols:
                gp_mean = gp_relative_means[feature]
                gp_mean_str = process_number(gp_mean)

                # Draw Cell
                pdf.draw_colored_rect(
                    canvas=canvas,
                    color=(1, 1, 1),  # type: ignore
                    x=table_cell_pose_left + table_cell_width,
                    y=y,
                    width=table_cell_width,
                    height=table_cell_height_pose,
                    fill=False,
                    stroke=True,
                    line_width=0.25,
                )

                # Then Draw Text
                pdf.draw_text(
                    canvas=canvas,
                    text=gp_mean_str,
                    x=table_cell_pose_left + table_cell_width + 2,
                    y=y - 2,
                    size=4,
                    font="Helvetica",
                )

                pt_mean = pt_relative_means[feature]
                pt_mean_str = process_number(pt_mean)

                pdf.draw_colored_rect(
                    canvas=canvas,
                    color=(1, 1, 1),  # type: ignore
                    x=table_cell_pose_left,
                    y=y,
                    width=table_cell_width,
                    height=table_cell_height_pose,
                    fill=False,
                    stroke=True,
                    line_width=0.25,
                )

                pdf.draw_text(
                    canvas=canvas,
                    text=pt_mean_str,
                    x=table_cell_pose_left + 2,
                    y=y - 2,
                    size=4,
                    font="Helvetica",
                )

                y = y + table_cell_height_pose

        case InterviewRole.INTERVIEWER:
            y = table_cell_int_pose_bot_start

            for feature in required_cols:
                gp_mean = gp_relative_means[feature]
                gp_mean_str = process_number(gp_mean)

                # Draw Cell
                pdf.draw_colored_rect(
                    canvas=canvas,
                    color=(1, 1, 1),  # type: ignore
                    x=table_cell_int_pose_left,
                    y=y,
                    width=table_cell_width,
                    height=table_cell_height_pose,
                    fill=False,
                    stroke=True,
                    line_width=0.25,
                )

                # Then Draw Text
                pdf.draw_text(
                    canvas=canvas,
                    text=gp_mean_str,
                    x=table_cell_int_pose_left + 2,
                    y=y - 2,
                    size=4,
                    font="Helvetica",
                )

                y = y + table_cell_height_pose

        case _:
            raise ValueError(f"Invalid role: {role}")


def get_z_score_params(
    z_score: float,
) -> Tuple[str, Tuple[float, float, float, float], str]:
    """
    Get formatting parameters for the z-score.

    Returns the z-score string, the color, and the font, where the color is based on the z-score.
    color is a tuple of (r, g, b, alpha).

    Args:
        z_score (float): The z-score.

    Returns:
        Tuple[str, Tuple[float, float, float, float], str]: The z-score string, the color,
            and the font.
    """
    grey = (0.5, 0.5, 0.5)
    empahsize_font = "Helvetica-Bold"
    default_font = "Helvetica"

    alpha = 0
    fill_color: Tuple[float, float, float] = (0, 0, 0)
    font = default_font

    # Determine Font and Fill Color
    # Check if not nan
    if np.isnan(z_score):
        z_score_str = ""
        fill_color = grey
        alpha = 0.5
        font = empahsize_font
    # Check if positive
    elif z_score > 0:
        z_score_str = f"+{z_score}"[:5]
        if z_score >= 1:
            fill_color = (z_score / 2.0, 0, 0)
            alpha = 0.5
            font = empahsize_font
    # Check if negative
    else:
        z_score_str = f"{z_score}"[:5]
        if z_score <= -1:
            fill_color = (0, 0, -z_score / 2.0)
            alpha = 0.5
            font = empahsize_font

    color = (*fill_color, alpha)

    return z_score_str, color, font  # type: ignore


def construct_fau_z_scores_table_by_role(
    canvas: canvas.Canvas,
    role: InterviewRole,
    interview_name: str,
    au_cols: List[str],
    data_path: Path,
    config_file: Path,
) -> None:
    """
    Constructs the FAU z-scores table for the video section.

    Reads the group means and stds from the metrics cache and compares
    them to the session pose means.

    The z-scores are calculated as:
    z_score = (session_mean - group_mean) / group_std

    The z-scores are then colored and placed in the table.

    Args:
        canvas (canvas.Canvas): The canvas to draw on.
        role (InterviewRole): The role for whom the FAU z-scores are being drawn.
        interview_name (str): The name of the interview.
        au_cols (List[str]): The list of AU columns.
        data_path (Path): The path to the data directory.
        config_file (Path): The path to the configuration file.

    Raises:
        ValueError: If the role is invalid.
        FileNotFoundError: If the metrics cache is not found.

    Returns:
        None
    """
    dp_dast_dict = dpdash.parse_dpdash_name(interview_name)
    study_id = dp_dast_dict["study"]
    subject_id = dp_dast_dict["subject"]

    session_of_pose_features = data.fetch_openface_features(
        interview_name=interview_name,
        subject_id=subject_id,
        study_id=study_id,
        role=role,
        cols=au_cols,
        config_file=config_file,
    )
    session_pose_means = session_of_pose_features.mean(axis=0)

    # Read Group Metrics (Mean and Std) from cached file
    group_means_and_std_path = data_path / f"metrics_cache_{role}.csv"

    if not group_means_and_std_path.exists():
        raise FileNotFoundError(
            f"Metrics cache not found at {group_means_and_std_path}"
        )

    group_means_and_std = pd.read_csv(group_means_and_std_path)

    # Only keep AU columns
    group_means_and_std = group_means_and_std[au_cols]

    # Compute Z Score
    group_means = group_means_and_std.iloc[0]
    group_stds = group_means_and_std.iloc[1]

    # Z Score = Mean - Group Mean / Group Std
    group_z_scores = (session_pose_means - group_means) / group_stds

    if role is InterviewRole.SUBJECT:
        # Compute Subject Z Score
        subject_of_pose_features = data.fetch_openface_subject_distribution(
            subject_id=subject_id,
            cols=au_cols,
            config_file=config_file,
        )
        subject_pose_means = subject_of_pose_features.mean(axis=0)
        subject_pose_std = subject_of_pose_features.std(axis=0)

        # Compute Subject Z Score
        subject_z_scores = (session_pose_means - subject_pose_means) / subject_pose_std

    # Write to Canvas
    table_cell_width = 18.05
    table_cell_height_fau = 7.47

    table_cell_fau_bot_start = 231.88
    table_cell_fau_left = 597.02

    table_cell_int_fau_left = 607
    table_cell_int_fau_bot_start = 486

    match role:
        case InterviewRole.SUBJECT:
            x = table_cell_fau_left
            y = table_cell_fau_bot_start
        case InterviewRole.INTERVIEWER:
            x = table_cell_int_fau_left
            y = table_cell_int_fau_bot_start
        case _:
            raise ValueError(f"Invalid role: {role}")

    for fau_label in au_cols:
        group_z_score = group_z_scores[fau_label]

        # Group Z Score
        z_score_str, color, font = get_z_score_params(group_z_score)
        # Draw Cell
        pdf.draw_colored_rect(
            canvas=canvas,
            color=color,
            x=x,
            y=y,
            width=table_cell_width,
            height=table_cell_height_fau,
            fill=True,
            stroke=True,
            line_width=0.25,
        )

        # Then Draw Text
        pdf.draw_text(
            canvas=canvas,
            text=z_score_str,
            x=x + 3,
            y=y - 2,
            size=4,
            font=font,
        )

        if role is InterviewRole.SUBJECT:
            # Subject Z Score
            subject_z_score = subject_z_scores[fau_label]  # type: ignore
            z_score_str, color, font = get_z_score_params(subject_z_score)
            # Draw Cell
            pdf.draw_colored_rect(
                canvas=canvas,
                color=color,
                x=x + table_cell_width,
                y=y,
                width=table_cell_width,
                height=table_cell_height_fau,
                fill=True,
                stroke=True,
                line_width=0.25,
            )

            # Then Draw Text
            pdf.draw_text(
                canvas=canvas,
                text=z_score_str,
                x=x + table_cell_width + 3,
                y=y - 2,
                size=4,
                font=font,
            )

        y = y + table_cell_height_fau


def draw_fau_table_header(canvas: canvas.Canvas, role: InterviewRole) -> None:
    """
    Writes the header for the FAU z-scores table.

    Args:
        canvas (canvas.Canvas): The canvas to draw on.
        role (InterviewRole): The role for whom the header is being drawn.

    Raises:
        ValueError: If the role is invalid.

    Returns:
        None
    """
    table_cell_height_fau = 7.47

    table_text_left_vid = 600
    table_text_bot_vid = 223.96

    table_text_left_vid2 = 616.4

    table_cell_int_fau_left = 607
    table_cell_int_fau_bot_start = 486

    table_text_left_vid_int = table_cell_int_fau_left + 4
    table_text_bot_vid_int = table_cell_int_fau_bot_start - table_cell_height_fau - 1

    match role:
        case InterviewRole.SUBJECT:
            pdf.draw_text(
                canvas=canvas,
                text="Group",
                x=table_text_left_vid,
                y=table_text_bot_vid - 2.5,
                size=2.7,
                font="Helvetica",
            )
            pdf.draw_text(
                canvas=canvas,
                text="Z-score",
                x=table_text_left_vid - 1,
                y=table_text_bot_vid,
                size=2.7,
                font="Helvetica",
            )

            pdf.draw_text(
                canvas=canvas,
                text="Participant",
                x=table_text_left_vid2,
                y=table_text_bot_vid - 2.5,
                size=2.7,
                font="Helvetica",
            )
            pdf.draw_text(
                canvas=canvas,
                text="Z-score",
                x=table_text_left_vid2 + 2,
                y=table_text_bot_vid,
                size=2.7,
                font="Helvetica",
            )
        case InterviewRole.INTERVIEWER:
            pdf.draw_text(
                canvas=canvas,
                text="Group",
                x=table_text_left_vid_int,
                y=table_text_bot_vid_int - 2.5,
                size=2.7,
                font="Helvetica",
            )
            pdf.draw_text(
                canvas=canvas,
                text="Z-score",
                x=table_text_left_vid_int - 1,
                y=table_text_bot_vid_int,
                size=2.7,
                font="Helvetica",
            )
        case _:
            raise ValueError(f"Invalid role: {role}")
