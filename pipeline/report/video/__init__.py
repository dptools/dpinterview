# package level init file
"""
Appearance and Movement Section
"""

from datetime import timedelta
from pathlib import Path
from typing import List

from reportlab.pdfgen import canvas

from pipeline.helpers import pdf, utils
from pipeline.models.interview_roles import InterviewRole
from pipeline.models.lite.cluster_bar_config import ClusterBarsConfig
from pipeline.models.lite.interview_metadata import InterviewMetadata
from pipeline.models.lite.ticks_config import TicksConfig
from pipeline.report import common
from pipeline.report.video import corr_matrix, heatmaps, qc

console = utils.get_console()


def construct_am_by_role(
    canvas: canvas.Canvas,
    role: InterviewRole,
    interview_name: str,
    start_time: timedelta,
    end_time: timedelta,
    frame_frequency: timedelta,
    interview_metadata: InterviewMetadata,
    config_file: Path,
    min_labels: List[str],
    heatmap_vid_pose_path: Path,
    heatmap_vid_fau_path: Path,
    corr_matrix_path: Path,
    assets_path: Path,
    headpose_labels: List[str],
    gaze_labels: List[str],
    au_labels: List[str],
    pose_cols: List[str],
    gaze_cols: List[str],
    au_cols: List[str],
    ticks_config: TicksConfig,
    cluster_bars_config: ClusterBarsConfig,
    data_path: Path,
) -> None:
    """
    Construct the Appearance and Movement section for a given role.

    Args:
        canvas (canvas.Canvas): The canvas to draw on.
        role (InterviewRole): The role for whom the section is being constructed.
        interview_name (str): The name of the interview.
        start_time (timedelta): The start time of the section.
        end_time (timedelta): The end time of the section.
        frame_frequency (timedelta): The frequency of the frames (snapshot bar).
        interview_metadata (InterviewMetadata): The interview metadata.
        config_file (Path): The path to the configuration file.
        min_labels (List[str]): The minute labels for the ticks.
        heatmap_vid_pose_path (Path): The path to the pose heatmap.
        heatmap_vid_fau_path (Path): The path to the FAU heatmap.
        corr_matrix_path (Path): The path to the correlation matrix.
        assets_path (Path): The path to the assets directory.
        headpose_labels (List[str]): The list of headpose labels.
        gaze_labels (List[str]): The list of gaze labels.
        au_labels (List[str]): The list of AU labels.
        pose_cols (List[str]): The list of pose columns.
        gaze_cols (List[str]): The list of gaze columns.
        au_cols (List[str]): The list of AU columns.
        ticks_config (TicksConfig): The ticks configuration.
        cluster_bars_config (ClusterBarsConfig): The cluster bars configuration.
        data_path (Path): The path to the data directory.

    Returns:
        None
    """
    pose_label_space = 7.48
    au_label_space = 7.45

    heatmap_label_right = 60.8
    sample_left = 641

    heatmaps.draw_pose_svgs_by_role(assets_path=assets_path, canvas=canvas, role=role)

    heatmaps.construct_heatmap_by_role(
        canvas=canvas,
        role=role,
        interview_name=interview_name,
        start_time=start_time,
        end_time=end_time,
        frame_frequency=frame_frequency,
        heatmap_vid_pose_path=heatmap_vid_pose_path,
        heatmap_vid_fau_path=heatmap_vid_fau_path,
        pose_labels=headpose_labels,
        gaze_labels=gaze_labels,
        au_labels=au_labels,
        min_labels=min_labels,
        pose_label_space=pose_label_space,
        au_label_space=au_label_space,
        heatmap_label_right=heatmap_label_right,
        ticks_config=ticks_config,
        cluster_bars_config=cluster_bars_config,
        assets_path=assets_path,
        config_file=config_file,
    )

    corr_matrix.contruct_dendrogram_by_role(
        canvas=canvas,
        role=role,
        assets_path=assets_path,
    )

    corr_matrix.construct_corr_matrix_by_role(
        canvas=canvas,
        role=role,
        interview_metadata=interview_metadata,
        corr_matrix_path=corr_matrix_path,
    )

    qc.construct_openface_metadata_box_by_role(
        canvas=canvas,
        role=role,
        interview_name=interview_name,
        config_file=config_file,
    )

    qc.draw_qc_metrics_by_role(
        canvas=canvas,
        role=role,
        interview_name=interview_name,
        x=sample_left,
        config_file=config_file,
    )

    corr_matrix.construct_pose_mean_tables_by_role(
        canvas=canvas,
        role=role,
        interview_name=interview_name,
        config_file=config_file,
        data_path=data_path,
        pose_cols=pose_cols,
        gaze_cols=gaze_cols,
    )

    corr_matrix.draw_fau_table_header(
        canvas=canvas,
        role=role,
    )

    corr_matrix.construct_fau_z_scores_table_by_role(
        canvas=canvas,
        role=role,
        interview_name=interview_name,
        au_cols=au_cols,
        data_path=data_path,
        config_file=config_file,
    )

    qc.construct_sample_image_by_role(
        canvas=canvas,
        interview_name=interview_name,
        role=role,
        config_file=config_file,
        start_time=start_time,
        end_time=end_time,
    )


def construct_am_report(
    canvas: canvas.Canvas,
    interview_name: str,
    start_time: timedelta,
    end_time: timedelta,
    frame_frequency: timedelta,
    interview_metadata: InterviewMetadata,
    config_file: Path,
    min_labels: List[str],
    heatmap_vid_pose_pt_path: Path,
    heatmap_vid_fau_pt_path: Path,
    heatmap_vid_pose_int_path: Path,
    heatmap_vid_fau_int_path: Path,
    corr_vid_pt_path: Path,
    corr_vid_int_path: Path,
    assets_path: Path,
    headpose_labels: List[str],
    gaze_labels: List[str],
    au_labels: List[str],
    pose_cols: List[str],
    gaze_cols: List[str],
    au_cols: List[str],
    ticks_config: TicksConfig,
    cluster_bars_config: ClusterBarsConfig,
    data_path: Path,
) -> None:
    """
    Construct the Appearance and Movement section for the report.

    Args:
        canvas (canvas.Canvas): The canvas to draw on.
        interview_name (str): The name of the interview.
        start_time (timedelta): The start time of the section.
        end_time (timedelta): The end time of the section.
        frame_frequency (timedelta): The frequency of the frames (snapshot bar).
        interview_metadata (InterviewMetadata): The interview metadata.
        config_file (Path): The path to the configuration file.
        min_labels (List[str]): The minute labels for the ticks.
        heatmap_vid_pose_pt_path (Path): The path to the pose heatmap for the participant.
        heatmap_vid_fau_pt_path (Path): The path to the FAU heatmap for the participant.
        heatmap_vid_pose_int_path (Path): The path to the pose heatmap for the interviewer.
        heatmap_vid_fau_int_path (Path): The path to the FAU heatmap for the interviewer.
        corr_vid_pt_path (Path): The path to the correlation matrix for the participant.
        corr_vid_int_path (Path): The path to the correlation matrix for the interviewer.
        assets_path (Path): The path to the assets directory.
        headpose_labels (List[str]): The list of headpose labels.
        gaze_labels (List[str]): The list of gaze labels.
        au_labels (List[str]): The list of AU labels.
        pose_cols (List[str]): The list of pose columns.
        gaze_cols (List[str]): The list of gaze columns.
        au_cols (List[str]): The list of AU columns.
        ticks_config (TicksConfig): The ticks configuration.
        cluster_bars_config (ClusterBarsConfig): The cluster bars configuration.
        data_path (Path): The path to the data directory.

    Returns:
        None
    """
    vid_header_bottom = 115.08
    bars_height = 31.7
    video_header = "Appearance & Movement"

    # Draw Video Header
    pdf.draw_colored_rect(
        canvas=canvas,
        x=0,
        y=vid_header_bottom,
        width=pdf.letter[1] * 2,
        height=bars_height,
        color=(0.7, 0.7, 0.7),  # type: ignore
        fill=True,
        stroke=True,
    )
    pdf.draw_text(
        canvas=canvas,
        text=video_header,
        x=None,
        y=(vid_header_bottom - bars_height / float(3.5)),
        size=14,
        font="Helvetica-Bold",
        center=True,
    )

    construct_am_by_role(
        canvas=canvas,
        role=InterviewRole.SUBJECT,
        interview_name=interview_name,
        start_time=start_time,
        end_time=end_time,
        frame_frequency=frame_frequency,
        interview_metadata=interview_metadata,
        config_file=config_file,
        min_labels=min_labels,
        heatmap_vid_pose_path=heatmap_vid_pose_pt_path,
        heatmap_vid_fau_path=heatmap_vid_fau_pt_path,
        corr_matrix_path=corr_vid_pt_path,
        assets_path=assets_path,
        headpose_labels=headpose_labels,
        gaze_labels=gaze_labels,
        au_labels=au_labels,
        pose_cols=pose_cols,
        gaze_cols=gaze_cols,
        au_cols=au_cols,
        ticks_config=ticks_config,
        cluster_bars_config=cluster_bars_config,
        data_path=data_path,
    )

    if interview_metadata.has_interviewer_stream:
        construct_am_by_role(
            canvas=canvas,
            role=InterviewRole.INTERVIEWER,
            interview_name=interview_name,
            start_time=start_time,
            end_time=end_time,
            frame_frequency=frame_frequency,
            interview_metadata=interview_metadata,
            config_file=config_file,
            min_labels=min_labels,
            heatmap_vid_pose_path=heatmap_vid_pose_int_path,
            heatmap_vid_fau_path=heatmap_vid_fau_int_path,
            corr_matrix_path=corr_vid_int_path,
            assets_path=assets_path,
            headpose_labels=headpose_labels,
            gaze_labels=gaze_labels,
            au_labels=au_labels,
            pose_cols=pose_cols,
            gaze_cols=gaze_cols,
            au_cols=au_cols,
            ticks_config=ticks_config,
            cluster_bars_config=cluster_bars_config,
            data_path=data_path,
        )

    common.draw_heatmap_legend(
        assets_path=assets_path, canvas=canvas, data_type="video"
    )
    common.draw_corr_matrix_legend(
        assets_path=assets_path, canvas=canvas, data_type="video"
    )
