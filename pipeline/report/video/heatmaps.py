"""
Contains functions to construct heatmaps for Appearance and Movement
section of the report.
"""

import tempfile
from datetime import timedelta
from pathlib import Path
from typing import List, Optional

from reportlab.pdfgen import canvas

from pipeline import constants, core
from pipeline.helpers import image, pdf, utils
from pipeline.models.interview_roles import InterviewRole
from pipeline.models.lite.cluster_bar_config import ClusterBarsConfig
from pipeline.models.lite.frame_request import FrameRequest
from pipeline.models.lite.ticks_config import TicksConfig
from pipeline.report import common

console = utils.get_console()


def draw_pose_svgs_by_role(
    assets_path: Path, canvas: canvas.Canvas, role: InterviewRole
) -> None:
    """
    Draws Pose related SVGs on the canvas based on the role.

    Args:
        assets_path (Path): The path to the assets directory.
        canvas (canvas.Canvas): The canvas to draw on.
        role (InterviewRole): The role of the primary person in the video.

    Raises:
        ValueError: If the role is invalid.

    Returns:
        None
    """
    pose_logos_left = 569.0
    pose_logos_width = 17.687
    pose_logos_height = 9.826

    head_pos_pt_bottom = 176.07
    head_ang_pt_bottom = 196.77
    gaze_dir_pt_bottom = 214.31

    head_pos_int_bottom = 432.58
    head_ang_int_bottom = 452.78
    gaze_dir_int_bottom = 470.82

    head_pos_path = assets_path / "svgs" / "head_pos.svg"
    head_ang_path = assets_path / "svgs" / "head_ang.svg"
    gaze_dir_path = assets_path / "svgs" / "gaze_dir.svg"

    svg_paths = [head_pos_path, head_ang_path, gaze_dir_path]
    scales = [1.7, 1.5, 1]
    xs = [pose_logos_left, pose_logos_left + 5, pose_logos_left]

    match role:
        case InterviewRole.SUBJECT:
            ys = [head_pos_pt_bottom, head_ang_pt_bottom, gaze_dir_pt_bottom]
        case InterviewRole.INTERVIEWER:
            ys = [head_pos_int_bottom, head_ang_int_bottom, gaze_dir_int_bottom]
        case _:
            raise ValueError(f"Invalid role: {role}")

    for svg, scale, x, y in zip(svg_paths, scales, xs, ys):
        pdf.draw_svg(
            canvas, svg, x, y, pose_logos_width * scale, pose_logos_height * scale
        )


def construct_subject_heatmap_headers_lines(
    canvas: canvas.Canvas,
    pose_labels: List[str],
    gaze_labels: List[str],
    au_labels: List[str],
    pose_label_space: float,
    au_label_space: float,
    heatmap_label_right: float,
) -> None:
    """
    Writes the headers and lines for the Subject's heatmap section.

    Args:
        canvas (canvas.Canvas): The canvas to draw on.
        pose_labels (List[str]): The list of pose labels.
        gaze_labels (List[str]): The list of gaze labels.
        au_labels (List[str]): The list of AU labels.
        pose_label_space (float): The space between pose labels.
        au_label_space (float): The space between AU labels.
        heatmap_label_right (float): The rightmost position of the heatmap labels.

    Returns:
        None
    """
    pt_pose_label_bottom_start = 163.08
    pt_au_label_bottom_start = 230.56

    heatmap_vid_pt_bottom_fau = 351.13
    pt_snapshot_bottom = 153.89

    pt_pose_line_start = pt_pose_label_bottom_start - 5
    pt_pose_line_end = (
        pt_pose_label_bottom_start - 5 + len(pose_labels) * pose_label_space
    )

    pt_gaze_line_start = pt_pose_line_end + 1
    pt_gaze_line_end = pt_gaze_line_start + len(gaze_labels) * pose_label_space

    pt_fau_line_start = pt_au_label_bottom_start - 5
    pt_fau_line_end = pt_fau_line_start + len(au_labels) * au_label_space

    gaze_labels_left = pdf.compute_x_right_align_list(
        gaze_labels, "Helvetica", 5, heatmap_label_right
    )
    pose_labels_left = pdf.compute_x_right_align_list(
        pose_labels, "Helvetica", 5, heatmap_label_right
    )
    au_labels_left = pdf.compute_x_right_align_list(
        au_labels, "Helvetica", 5, heatmap_label_right
    )

    pose_line_hor = min(pose_labels_left) - 1.5
    gaze_line_hor = min(gaze_labels_left) - 1.5
    fau_line_hor = min(au_labels_left) - 1.5

    label_line_hor = min(pose_line_hor, gaze_line_hor, fau_line_hor)

    pdf.draw_line(
        canvas,
        pose_line_hor,
        pt_pose_line_start,
        label_line_hor,
        pt_pose_line_end,
        0.25,
    )
    pdf.draw_line(
        canvas,
        pose_line_hor,
        pt_gaze_line_start,
        label_line_hor,
        pt_gaze_line_end,
        0.25,
    )
    pdf.draw_line(
        canvas, pose_line_hor, pt_fau_line_start, label_line_hor, pt_fau_line_end, 0.25
    )

    pdf.draw_text_vertical_centered(
        canvas,
        constants.HEATMAP_VID_POSE_HEADER,
        pt_pose_line_start,
        pt_pose_line_end,
        label_line_hor - 3,
        5,
        "Helvetica",
    )

    pdf.draw_text_vertical_centered(
        canvas,
        constants.HEATMAP_VID_GAZE_HEADER,
        pt_gaze_line_start,
        pt_gaze_line_end,
        label_line_hor - 3,
        5,
        "Helvetica",
    )

    pdf.draw_text_vertical_centered(
        canvas,
        constants.HEATMAP_VID_FAUS_HEADER,
        pt_fau_line_start,
        pt_fau_line_end,
        label_line_hor - 3,
        5,
        "Helvetica",
    )

    pdf.draw_text_vertical_centered(
        canvas,
        "Participant",
        pt_snapshot_bottom,
        heatmap_vid_pt_bottom_fau,
        label_line_hor - 15,
        8,
        "Helvetica-Bold",
    )


def construct_int_heatmap_headers_lines(
    canvas: canvas.Canvas,
    pose_labels: List[str],
    gaze_labels: List[str],
    au_labels: List[str],
    pose_label_space: float,
    au_label_space: float,
    heatmap_label_right: float,
):
    """
    Writes the headers and lines for the Interviewer's heatmap section.

    Args:
        canvas (canvas.Canvas): The canvas to draw on.
        pose_labels (List[str]): The list of pose labels.
        gaze_labels (List[str]): The list of gaze labels.
        au_labels (List[str]): The list of AU labels.
        pose_label_space (float): The space between pose labels.
        au_label_space (float): The space between AU labels.
        heatmap_label_right (float): The rightmost position of the heatmap labels.

    Returns:
        None
    """
    int_pose_label_bottom_start = 417.22
    int_au_label_bottom_start = 484.5

    heatmap_vid_int_bottom_fau = 605.11
    int_snapshot_bottom = 408.98

    int_pose_line_start = int_pose_label_bottom_start - 5
    int_pose_line_end = (
        int_pose_label_bottom_start - 5 + len(pose_labels) * pose_label_space
    )

    int_gaze_line_start = int_pose_line_end + 1
    int_gaze_line_end = int_gaze_line_start + len(gaze_labels) * pose_label_space

    int_fau_line_start = int_au_label_bottom_start - 5
    int_fau_line_end = int_fau_line_start + len(au_labels) * au_label_space

    gaze_labels_left = pdf.compute_x_right_align_list(
        gaze_labels, "Helvetica", 5, heatmap_label_right
    )
    pose_labels_left = pdf.compute_x_right_align_list(
        pose_labels, "Helvetica", 5, heatmap_label_right
    )
    au_labels_left = pdf.compute_x_right_align_list(
        au_labels, "Helvetica", 5, heatmap_label_right
    )

    pose_line_hor = min(pose_labels_left) - 1.5
    gaze_line_hor = min(gaze_labels_left) - 1.5
    fau_line_hor = min(au_labels_left) - 1.5

    label_line_hor = min(pose_line_hor, gaze_line_hor, fau_line_hor)

    pdf.draw_line(
        canvas,
        pose_line_hor,
        int_pose_line_start,
        label_line_hor,
        int_pose_line_end,
        0.25,
    )
    pdf.draw_line(
        canvas,
        pose_line_hor,
        int_gaze_line_start,
        label_line_hor,
        int_gaze_line_end,
        0.25,
    )
    pdf.draw_line(
        canvas,
        pose_line_hor,
        int_fau_line_start,
        label_line_hor,
        int_fau_line_end,
        0.25,
    )

    pdf.draw_text_vertical_centered(
        canvas,
        constants.HEATMAP_VID_POSE_HEADER,
        int_pose_line_start,
        int_pose_line_end,
        label_line_hor - 3,
        5,
        "Helvetica",
    )

    pdf.draw_text_vertical_centered(
        canvas,
        constants.HEATMAP_VID_GAZE_HEADER,
        int_gaze_line_start,
        int_gaze_line_end,
        label_line_hor - 3,
        5,
        "Helvetica",
    )

    pdf.draw_text_vertical_centered(
        canvas,
        constants.HEATMAP_VID_FAUS_HEADER,
        int_fau_line_start,
        int_fau_line_end,
        label_line_hor - 3,
        5,
        "Helvetica",
    )

    pdf.draw_text_vertical_centered(
        canvas,
        "Interviewer",
        int_snapshot_bottom,
        heatmap_vid_int_bottom_fau,
        label_line_hor - 15,
        8,
        "Helvetica-Bold",
    )


def construct_heatmap_ticks_by_role(
    canvas: canvas.Canvas,
    x: float,
    ticks_config: TicksConfig,
    min_labels: List[str],
    role: InterviewRole,
) -> None:
    """
    Draws the ticks and minute labels for the heatmap section for ach role.

    Args:
        canvas (canvas.Canvas): The canvas to draw on.
        x (float): The x position of the ticks.
        ticks_config (TicksConfig): The ticks configuration.
        min_labels (List[str]): The list of minute labels.
        role (InterviewRole): For whom the ticks are being drawn.
            - Top section is for InterviewRole.SUBJECT
            - Bottom ticks is for InterviewRole.INTERVIEWE

    Raises:
        ValueError: If the role is invalid.

    Returns:
        None
    """
    heatmap_vid_pt_bottom_fau = 351.13
    heatmap_vid_int_bottom_fau = 605.11

    min_label_offset_ticks_x = -4  # minute text is centered around tick

    minutes_start_x = min_label_offset_ticks_x + x

    tick_left = x

    match role:
        case InterviewRole.SUBJECT:
            y = heatmap_vid_pt_bottom_fau
        case InterviewRole.INTERVIEWER:
            y = heatmap_vid_int_bottom_fau
        case _:
            raise ValueError(f"Invalid role: {role}")

    common.construct_heatmap_ticks(
        canvas=canvas,
        y=y,
        ticks_config=ticks_config,
        min_labels=min_labels,
        minutes_start_x=minutes_start_x,
        tick_left=tick_left,
    )


def construct_heatmap_labels_by_role(
    canvas: canvas.Canvas,
    pose_labels: List[str],
    gaze_labels: List[str],
    au_labels: List[str],
    pose_label_space: float,
    heatmap_label_right: float,
    au_label_space: float,
    role: InterviewRole,
) -> None:
    """
    Writes the labels for the heatmap section for each role.

    Args:
        canvas (canvas.Canvas): The canvas to draw on.
        pose_labels (List[str]): The list of pose labels.
        gaze_labels (List[str]): The list of gaze labels.
        au_labels (List[str]): The list of AU labels.
        pose_label_space (float): The space between pose labels.
        heatmap_label_right (float): The rightmost position of the heatmap labels.
        au_label_space (float): The space between AU labels.
        role (InterviewRole): For whom the labels are being drawn.

    Raises:
        ValueError: If the role is invalid.

    Returns:
        None
    """
    pt_pose_label_bottom_start = 163.08
    pt_au_label_bottom_start = 230.56

    int_pose_label_bottom_start = 417.22
    int_au_label_bottom_start = 484.5

    pose_labels_left = pdf.compute_x_right_align_list(
        pose_labels, "Helvetica", 5, heatmap_label_right
    )
    gaze_labels_left = pdf.compute_x_right_align_list(
        gaze_labels, "Helvetica", 5, heatmap_label_right
    )
    au_labels_left = pdf.compute_x_right_align_list(
        au_labels, "Helvetica", 5, heatmap_label_right
    )

    match role:
        case InterviewRole.SUBJECT:
            cur_bot = pt_pose_label_bottom_start
        case InterviewRole.INTERVIEWER:
            cur_bot = int_pose_label_bottom_start
        case _:
            raise ValueError(f"Invalid role: {role}")

    for text, left in zip(pose_labels, pose_labels_left):
        pdf.draw_text(canvas, text, left, cur_bot, 5, "Helvetica")
        cur_bot = cur_bot + pose_label_space
    for text, left in zip(gaze_labels, gaze_labels_left):
        pdf.draw_text(canvas, text, left, cur_bot, 5, "Helvetica")
        cur_bot = cur_bot + pose_label_space

    match role:
        case InterviewRole.SUBJECT:
            cur_bot = pt_au_label_bottom_start
        case InterviewRole.INTERVIEWER:
            cur_bot = int_au_label_bottom_start
        case _:
            raise ValueError(f"Invalid role: {role}")

    for text, left in zip(au_labels, au_labels_left):
        pdf.draw_text(canvas, text, left, cur_bot, 5, "Helvetica")
        cur_bot = cur_bot + au_label_space


def draw_fau_logos(
    canvas: canvas.Canvas, role: InterviewRole, au_labels: List[str], assets_path: Path
):
    """
    Draws the FAU images for the heatmap section.

    Args:
        canvas (canvas.Canvas): The canvas to draw on.
        role (InterviewRole): The role of the primary person in the video.
        au_labels (List[str]): The list of AU labels.
        assets_path (Path): The path to the assets directory.

    Raises:
        ValueError: If the role is invalid.
    """
    au_assets_path = assets_path / "FAU_sample_images"

    au_samples_left = 568.46
    au_samples_width = 19.249
    au_samples_height = 6.303
    au_samples_int_start = 484.35
    au_samples_pt_start = 230.77
    au_samples_incr = 7.5

    match role:
        case InterviewRole.SUBJECT:
            cur_bot = au_samples_pt_start
        case InterviewRole.INTERVIEWER:
            cur_bot = au_samples_int_start
        case _:
            raise ValueError(f"Invalid role: {role}")

    for fau in au_labels:
        # Get only last 2 digits of FAU
        file_name = f"{fau[-2:]}.png"
        fau_path = au_assets_path / file_name
        pdf.draw_image(
            canvas,
            fau_path,
            au_samples_left,
            cur_bot,
            au_samples_width,
            au_samples_height,
        )
        cur_bot = cur_bot + au_samples_incr


def construct_heatmap_header_lines_by_role(
    canvas: canvas.Canvas,
    pose_labels: List[str],
    gaze_labels: List[str],
    au_labels: List[str],
    pose_label_space: float,
    au_label_space: float,
    heatmap_label_right: float,
    role: InterviewRole,
) -> None:
    """
    Draws the headers and lines for the heatmap section for each role.

    Args:
        canvas (canvas.Canvas): The canvas to draw on.
        pose_labels (List[str]): The list of pose labels.
        gaze_labels (List[str]): The list of gaze labels.
        au_labels (List[str]): The list of AU labels.
        pose_label_space (float): The space between pose labels.
        au_label_space (float): The space between AU labels.
        heatmap_label_right (float): The rightmost position of the heatmap labels.
        role (InterviewRole): For whom the headers and lines are being drawn.

    Raises:
        ValueError: If the role is invalid.

    Returns:
        None
    """

    match role:
        case InterviewRole.SUBJECT:
            construct_subject_heatmap_headers_lines(
                canvas=canvas,
                pose_labels=pose_labels,
                gaze_labels=gaze_labels,
                au_labels=au_labels,
                pose_label_space=pose_label_space,
                au_label_space=au_label_space,
                heatmap_label_right=heatmap_label_right,
            )
        case InterviewRole.INTERVIEWER:
            construct_int_heatmap_headers_lines(
                canvas=canvas,
                pose_labels=pose_labels,
                gaze_labels=gaze_labels,
                au_labels=au_labels,
                pose_label_space=pose_label_space,
                au_label_space=au_label_space,
                heatmap_label_right=heatmap_label_right,
            )
        case _:
            raise ValueError(f"Invalid role: {role}")


def draw_cluster_bars_vid(
    canvas: canvas.Canvas,
    cluster_bars_config: ClusterBarsConfig,
    role: InterviewRole,
) -> None:
    """
    Draw the cluster bars (colored vertival lines, to group FAUs)
    for the video section.

    Args:
        canvas (canvas.Canvas): The canvas to draw on.
        cluster_bars_config (ClusterBarsConfig): The cluster bars configuration.
        role (InterviewRole): The role for whom the cluster bars are being drawn

    Raises:
        ValueError: If the role is invalid.

    Returns:
        None
    """
    # cluster_bars_heights_vid = [21.693, 28.571, 21.693, 50]
    cluster_bar_height = 7.14
    cluster_fau_counts = [5, 4, 5, 3]
    cluster_bars_heights_vid = [
        cluster_bar_height * count for count in cluster_fau_counts
    ]

    green_color = (0, 0.4, 0)
    red_color = (1, 0, 0)
    # turquoise_color = (0, 1, 1)
    purple_color = (1, 0, 1)
    orange_color = (1, 0.5, 0)

    cluster_bars_colors_vid = [purple_color, red_color, green_color, orange_color]

    cluster_bars_vid1_left = 34.93
    cluster_bars_vid2_left = 591.26

    cluster_bars_pt_vid_bot_end = 350.22
    cluster_bars_int_vid_bot_end = 604.11

    match role:
        case InterviewRole.SUBJECT:
            cur_bot = cluster_bars_pt_vid_bot_end
        case InterviewRole.INTERVIEWER:
            cur_bot = cluster_bars_int_vid_bot_end
        case _:
            raise ValueError(f"Invalid role: {role}")

    for color, height in reversed(
        list(zip(cluster_bars_colors_vid, cluster_bars_heights_vid))
    ):
        pdf.draw_colored_rect(
            canvas=canvas,
            color=color,
            height=height,
            width=cluster_bars_config.cluster_bars_width,
            x=cluster_bars_vid2_left,
            y=cur_bot,
        )
        pdf.draw_colored_rect(
            canvas=canvas,
            color=color,
            height=height,
            width=cluster_bars_config.cluster_bars_width,
            x=cluster_bars_vid1_left,
            y=cur_bot,
        )
        cur_bot = cur_bot - height - cluster_bars_config.cluster_bars_space * 2.0


def construct_snapshots_bar(
    canvas: canvas.Canvas,
    frame_paths: List[Optional[Path]],
    role: InterviewRole,
    deidentified: bool = True,
):
    """
    Draws the snapshots bar for the video section. Multiple smaller images on the top middle.

    Args:
        canvas (canvas.Canvas): The canvas to draw on.
        frame_paths (List[Optional[Path]]): The list of frame paths.
        role (InterviewRole): The role for whom the snapshots are being drawn
        deidentified (bool): Whether the images are deidentified.

    Raises:
        ValueError: If the role is invalid.

    Returns:
        None
    """
    snapshot_height = 29
    snapshot_width = 29
    # num_snapshots = 15
    snapshot_start_left = 66
    snapshot_h_spacing = 33.5
    pt_snapshot_bottom = 153.89
    int_snapshot_bottom = 408.98

    no_data_color = (0.45, 0.45, 0.45)

    samples_text = "Sample Face Frames"

    samples_text_left = 10
    pt_samples_text_bottom = 137.9
    int_samples_text_bottom = 394.48

    match role:
        case InterviewRole.SUBJECT:
            y = pt_snapshot_bottom
            text_y = pt_samples_text_bottom
        case InterviewRole.INTERVIEWER:
            y = int_snapshot_bottom
            text_y = int_samples_text_bottom
        case _:
            raise ValueError(f"Invalid role: {role}")

    files: List[tempfile.NamedTemporaryFile] = []  # type: ignore

    x = snapshot_start_left
    for frame in frame_paths:
        if frame is None:
            pdf.draw_colored_rect(
                canvas=canvas,
                color=no_data_color,  # type: ignore
                height=snapshot_height,
                width=snapshot_width,
                x=x,
                y=y,
            )
        else:
            if deidentified:
                strategy = "filter_face_data"
                temp_file = tempfile.NamedTemporaryFile(suffix=".bmp")
                files.append(temp_file)

                match strategy:
                    case "blur":
                        image.blur_image(
                            source_image=frame, dest_image=Path(temp_file.name)
                        )
                        frame = temp_file.name

                    case "black_bar":
                        # temp_file = tempfile.NamedTemporaryFile(suffix=".bmp")
                        # temp_file_name = temp_file.name

                        image.draw_bars_over_image(
                            source_image=frame, dest_image=Path(temp_file.name)
                        )
                        frame = temp_file.name

                    case "filter_face_data":
                        image.filter_by_range(
                            source_image=frame,
                            dest_image=Path(temp_file.name),
                        )

                        frame = temp_file.name

            pdf.draw_image(canvas, Path(frame), x, y, snapshot_width, snapshot_height)

        x = x + snapshot_h_spacing

    for file in files:
        # Close and delete temp files
        file.close()

    pdf.draw_text(canvas, samples_text, samples_text_left, text_y, 4, "Helvetica")


# Place heatmaps
def place_heatmaps_by_role(
    canvas: canvas.Canvas,
    x: float,
    role: InterviewRole,
    heatmap_vid_pose_path: Path,
    heatmap_vid_fau_path: Path,
) -> None:
    """
    Places the heatmaps on the canvas based on the role.

    Args:
        canvas (canvas.Canvas): The canvas to draw on.
        x (float): The x position of the heatmaps.
        role (InterviewRole): The role for whom the heatmaps are being placed.
        heatmap_vid_pose_path (Path): The path to the pose heatmap.
        heatmap_vid_fau_path (Path): The path to the FAU heatmap.

    Raises:
        ValueError: If the role is invalid.

    Returns:
        None
    """
    # heatmap_vid_pose_crop = (10, 8, 1562, 217)

    heatmap_width = 503.949

    heatmap_vid_pt_bottom_pose = 218.57
    heatmap_vid_int_bottom_pose = 472.87
    heatmap_vid_height_pose = 61.099

    heatmap_vid_pt_bottom_fau = 351.13
    heatmap_vid_int_bottom_fau = 605.11
    heatmap_vid_height_fau = 127.271

    match role:
        case InterviewRole.SUBJECT:
            pose_y = heatmap_vid_pt_bottom_pose
            fau_y = heatmap_vid_pt_bottom_fau
        case InterviewRole.INTERVIEWER:
            pose_y = heatmap_vid_int_bottom_pose
            fau_y = heatmap_vid_int_bottom_fau
        case _:
            raise ValueError(f"Invalid role: {role}")

    # pose heatmap
    pdf.draw_image(
        canvas=canvas,
        image_path=heatmap_vid_pose_path,
        x=x,
        y=pose_y,
        width=heatmap_width,
        height=heatmap_vid_height_pose,
    )

    # fau heatmap
    pdf.draw_image(
        canvas=canvas,
        image_path=heatmap_vid_fau_path,
        x=x,
        y=fau_y,
        width=heatmap_width,
        height=heatmap_vid_height_fau,
    )


def construct_heatmap_by_role(
    canvas: canvas.Canvas,
    interview_name: str,
    role: InterviewRole,
    start_time: timedelta,
    end_time: timedelta,
    frame_frequency: timedelta,
    heatmap_vid_pose_path: Path,
    heatmap_vid_fau_path: Path,
    pose_labels: List[str],
    gaze_labels: List[str],
    au_labels: List[str],
    min_labels: List[str],
    pose_label_space: float,
    au_label_space: float,
    heatmap_label_right: float,
    ticks_config: TicksConfig,
    cluster_bars_config: ClusterBarsConfig,
    assets_path: Path,
    config_file: Path,
    deidentify: bool = True,
) -> None:
    """
    Constructs the heatmap section for the video report. Includes the headers, ticks, labels for
    the FAU, Pose + Gaze Heatmaps, Cluster Bars, and Snapshots.

    Args:
        canvas (canvas.Canvas): The canvas to draw on.
        interview_name (str): The name of the interview.
        role (InterviewRole): The role for whom the heatmaps are being drawn.
        start_time (timedelta): The start time of the video.
        end_time (timedelta): The end time of the video.
        frame_frequency (timedelta): The frequency of the frames (snapshot bar).
        heatmap_vid_pose_path (Path): The path to the pose heatmap.
        heatmap_vid_fau_path (Path): The path to the FAU heatmap.
        pose_labels (List[str]): The list of pose labels.
        gaze_labels (List[str]): The list of gaze labels.
        au_labels (List[str]): The list of AU labels.
        min_labels (List[str]): The list of minute labels.
        pose_label_space (float): The space between pose labels.
        au_label_space (float): The space between AU labels.
        heatmap_label_right (float): The rightmost position of the heatmap labels.
        ticks_config (TicksConfig): The ticks configuration.
        cluster_bars_config (ClusterBarsConfig): The cluster bars configuration.
        assets_path (Path): The path to the assets directory.
        config_file (Path): The path to the configuration file.
        deidentify (bool, optional): Whether to deidentify the images.
            Defaults to False.
            Deidentification is done by removing all face data from the images.

    Raises:
        ValueError: If the role is invalid.

    Returns:
        None
    """
    heatmap_left = 62.98

    construct_heatmap_header_lines_by_role(
        canvas=canvas,
        pose_labels=pose_labels,
        gaze_labels=gaze_labels,
        au_labels=au_labels,
        pose_label_space=pose_label_space,
        au_label_space=au_label_space,
        heatmap_label_right=heatmap_label_right,
        role=role,
    )

    construct_heatmap_ticks_by_role(
        canvas=canvas,
        x=heatmap_left,
        ticks_config=ticks_config,
        min_labels=min_labels,
        role=role,
    )

    construct_heatmap_labels_by_role(
        canvas=canvas,
        pose_labels=pose_labels,
        gaze_labels=gaze_labels,
        au_labels=au_labels,
        pose_label_space=pose_label_space,
        heatmap_label_right=heatmap_label_right,
        au_label_space=au_label_space,
        role=role,
    )

    place_heatmaps_by_role(
        canvas=canvas,
        x=heatmap_left,
        role=role,
        heatmap_vid_pose_path=heatmap_vid_pose_path,
        heatmap_vid_fau_path=heatmap_vid_fau_path,
    )

    draw_fau_logos(
        canvas=canvas,
        role=role,
        au_labels=au_labels,
        assets_path=assets_path,
    )

    draw_cluster_bars_vid(
        canvas=canvas,
        cluster_bars_config=cluster_bars_config,
        role=role,
    )

    with tempfile.TemporaryDirectory() as temp_dir:
        frame_numbers = FrameRequest.get_frame_numbers(
            interview_name=interview_name,
            role=role,
            start_time=start_time,
            end_time=end_time,
            frame_frequency=frame_frequency,
            config_file=config_file,
        )

        openface_overlaid_video_path = core.get_openfece_features_overlaid_video_path(
            config_file=config_file, interview_name=interview_name, role=role
        )

        if openface_overlaid_video_path is None:
            console.print(
                f"OpenFace overlaid video not found for {role.value}",
                style="error",
            )
            frame_paths: List[Path | None] = [None] * len(frame_numbers)
        else:
            frame_paths = image.get_frames_by_numbers(
                video_path=openface_overlaid_video_path,
                frame_numbers=frame_numbers,
                out_dir=Path(temp_dir),
            )

        construct_snapshots_bar(
            canvas=canvas,
            frame_paths=frame_paths,
            role=role,
            deidentified=deidentify,
        )
