"""
Generates a report for the Interview.
"""

import tempfile
from datetime import timedelta
from pathlib import Path
from typing import List, Optional

import numpy as np
import pandas as pd
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas

from pipeline import constants, core
from pipeline.helpers import dpdash, utils
from pipeline.helpers.config import config
from pipeline.helpers.plot import corr_matrix, heatmaps
from pipeline.models.interview_roles import InterviewRole
from pipeline.models.lite.interview_metadata import InterviewMetadata
from pipeline.report import common, header, video


def generate_report(
    interview_name: str,
    dest_file_name: Path,
    config_file: Path,
) -> Optional[str]:
    """
    Generates a report for the Interview.

    Args:
        interview_name (str): DPDash name of the Interview.
        dest_file_name (Path): Path to save the report.
        config_file (Path): Path to the config file.

    Returns:
        None if the report was generated successfully, else an error message.
    """
    console = utils.get_console()

    report_params = config(config_file, section="report_generation")
    fau_h_idx: List[int] = []
    bin_size = int(report_params["bin_size"])
    bins_per_page = int(report_params["bins_per_page"])
    anonymize_s = report_params["anonymize"].lower()

    # str to bool
    if anonymize_s == "true":
        anonymize = True
    else:
        anonymize = False

    console.log(f"Anonymize: {anonymize}")

    fau_gap_indices = report_params["fau_h_gap_idx"].split(",")
    for idx in fau_gap_indices:
        fau_h_idx.append(int(idx))

    dp_dash_dict = dpdash.parse_dpdash_name(interview_name)
    subject_id = dp_dash_dict["subject"]
    study_id = dp_dash_dict["study"]

    interview_metadata = InterviewMetadata.get(
        interview_name=interview_name, config_file=config_file
    )

    required_cols = (
        ["timestamp"]
        + constants.HEADPOSE_COLS
        + constants.GAZE_COLS
        + constants.AU_COLS
    )

    # Fetch OpenFace features for PT and INT from DB
    with console.status("Fetching OpenFace features...") as status:
        status.update("Fetching OpenFace features for subject...")
        of_pt_session = core.fetch_openface_features(
            interview_name=interview_name,
            subject_id=subject_id,
            study_id=study_id,
            role=InterviewRole.SUBJECT,
            cols=required_cols,
            config_file=config_file,
        )
        try:
            of_pt_session["timestamp"] = of_pt_session["timestamp"].apply(
                utils.datetime_time_to_float
            )
        except AttributeError:
            pass

        if interview_metadata.has_interviewer_stream:
            status.update("Fetching OpenFace features for interviewer...")
            of_int_session = core.fetch_openface_features(
                interview_name=interview_name,
                subject_id=subject_id,
                study_id=study_id,
                role=InterviewRole.INTERVIEWER,
                cols=required_cols,
                config_file=config_file,
            )
            try:
                of_int_session["timestamp"] = of_int_session["timestamp"].apply(
                    utils.datetime_time_to_float
                )
            except AttributeError:
                pass

        temp_files_common: List[tempfile.NamedTemporaryFile] = []  # type: ignore

        correlation_matrix_pt_path = tempfile.NamedTemporaryFile(suffix=".png")
        temp_files_common.append(correlation_matrix_pt_path)

        correlation_matrix_int_path = tempfile.NamedTemporaryFile(suffix=".png")
        temp_files_common.append(correlation_matrix_int_path)

        console.log("Generating correlation matrices...")
        status.update("Generating correlation for subject...")
        corr_matrix.generate_correlation_matric(
            interview_name=interview_name,
            role=InterviewRole.SUBJECT,
            output_path=correlation_matrix_pt_path,  # type: ignore
            heatmap_config=constants.heatmap_config,
            config_file_path=str(config_file),
            gap_idx=fau_h_idx,
            au_cols=constants.AU_COLS,
            data_path=constants.DATA_PATH,
        )

        status.update("Generating correlation for interviewer...")
        corr_matrix.generate_correlation_matric(
            interview_name=interview_name,
            role=InterviewRole.INTERVIEWER,
            output_path=correlation_matrix_int_path,  # type: ignore
            heatmap_config=constants.heatmap_config,
            config_file_path=str(config_file),
            gap_idx=fau_h_idx,
            au_cols=constants.AU_COLS,
            data_path=constants.DATA_PATH,
        )

        console.log("Starting report generation...")

        min_timestamp = of_pt_session["timestamp"].min()
        max_timestamp = of_pt_session["timestamp"].max()
        duration = max_timestamp - min_timestamp

        # Check if the duration is NaN
        if np.isnan(duration):
            message = "Skipping report generation. Duration is NaN."
            console.log(message)
            return message

        console.log(f"Interview duration: {duration:.2f} seconds")
        seconds_per_page = bin_size * bins_per_page

        status.update(f"Splitting Interview into {seconds_per_page} second chunks...")
        of_pt_session_parts = []
        of_int_session_parts = []

        for i in range(0, int(duration), seconds_per_page):
            of_pt_session_parts.append(
                of_pt_session[
                    (of_pt_session["timestamp"] >= i)
                    & (of_pt_session["timestamp"] < i + seconds_per_page)
                ]
            )
            if interview_metadata.has_interviewer_stream:
                of_int_session_parts.append(
                    of_int_session[  # type: ignore
                        (of_int_session["timestamp"] >= i)  # type: ignore
                        & (of_int_session["timestamp"] < i + seconds_per_page)  # type: ignore
                    ]
                )
            else:
                of_int_session_parts.append(None)

        num_pages = len(of_pt_session_parts)
        console.log(f"Report will have {num_pages} pages.")
        status.update("Starting report generation...")

        fau_metrics = pd.read_csv(constants.FAU_METRICS_PT_CACHE)
        # row1 has average of all the rows, row2 has standard deviation of all the rows
        fau_avgs = fau_metrics.iloc[0]
        fau_stds = fau_metrics.iloc[1]

        page_number: int = 1
        start_timestap: float = 0.0
        c = canvas.Canvas(filename=str(dest_file_name), pagesize=letter)

        page_number: int = 1
        start_timestap: float = 0.0

        for of_pt_part, of_int_part in zip(of_pt_session_parts, of_int_session_parts):
            console.log(f"Generating page {page_number} of {num_pages}...")
            duration = bin_size * bins_per_page

            # Compute frame frequency as the timesdelta per frame
            # required to get the desired number of frames
            num_frames = constants.NUM_FRAMES
            frame_frequency = duration / num_frames
            frame_frequency = timedelta(seconds=frame_frequency)

            end_timestamp = start_timestap + duration

            start_timedelta = timedelta(seconds=start_timestap)
            end_timedelta = timedelta(seconds=end_timestamp)

            num_labels = (
                int(bins_per_page / constants.heatmap_config.bins_per_v_gap) + 1
            )
            min_labels = utils.create_labels(start_timestap, end_timestamp, num_labels)

            bins = np.linspace(start_timestap, end_timestamp, bins_per_page + 1)

            # Split the data into bins
            # Reference:
            # https://stackoverflow.com/questions/20625582/how-to-deal-with-settingwithcopywarning-in-pandas
            of_pt_part = of_pt_part.iloc[:]  # To avoid SettingWithCopyWarning
            of_pt_part["bin"] = pd.cut(
                of_pt_part["timestamp"], bins=bins, labels=False  # type: ignore
            )
            if of_int_part is not None:
                of_int_part = of_int_part.iloc[:]  # To avoid SettingWithCopyWarning
                of_int_part["bin"] = pd.cut(
                    of_int_part["timestamp"], bins=bins, labels=False  # type: ignore
                )  # type: ignore

            temp_files: List[tempfile.NamedTemporaryFile] = []  # type: ignore

            # Generate pose and gaze heatmaps
            # features = constants.HEADPOSE_FEATURES + constants.GAZE_FEATURES
            cols = constants.HEADPOSE_COLS + constants.GAZE_COLS

            heatmap_vid_pose_gaze_pt = tempfile.NamedTemporaryFile(suffix=".png")
            temp_files.append(heatmap_vid_pose_gaze_pt)

            heatmap_vid_pose_gaze_int = tempfile.NamedTemporaryFile(suffix=".png")
            temp_files.append(heatmap_vid_pose_gaze_int)

            status.update("Generating Pose Heatmap for subject...")
            heatmaps.make_heatmap(
                df=of_pt_part,
                num_bins=bins_per_page,
                cols=cols,
                output_path=heatmap_vid_pose_gaze_pt,  # type: ignore
                heatmap_config=constants.heatmap_config,
            )

            if of_int_part is not None:
                status.update("Generating Pose Heatmap for interviewer...")
                heatmaps.make_heatmap(
                    df=of_int_part,
                    num_bins=bins_per_page,
                    cols=cols,
                    output_path=heatmap_vid_pose_gaze_int,  # type: ignore
                    heatmap_config=constants.heatmap_config,
                )

            # Generate AU heatmaps
            heatmap_vid_fau_pt = tempfile.NamedTemporaryFile(suffix=".png")
            temp_files.append(heatmap_vid_fau_pt)

            heatmap_vid_fau_int = tempfile.NamedTemporaryFile(suffix=".png")
            temp_files.append(heatmap_vid_fau_int)

            status.update("Generating AU Heatmap for subject...")
            heatmaps.make_standard_deviation_heatmap(
                df=of_pt_part,
                fau_avgs=fau_avgs,
                fau_stds=fau_stds,
                num_bins=bins_per_page,
                features=constants.AU_LABELS,
                cols=constants.AU_COLS,
                output_path=heatmap_vid_fau_pt,  # type: ignore
                h_gap_idx=fau_h_idx,
                heatmap_config=constants.heatmap_config,
            )
            status.update("Generating AU Heatmap for interviewer...")
            if of_int_part is not None:
                heatmaps.make_standard_deviation_heatmap(
                    df=of_int_part,
                    fau_avgs=fau_avgs,
                    fau_stds=fau_stds,
                    num_bins=bins_per_page,
                    features=constants.AU_LABELS,
                    cols=constants.AU_COLS,
                    output_path=heatmap_vid_fau_int,  # type: ignore
                    h_gap_idx=fau_h_idx,
                    heatmap_config=constants.heatmap_config,
                )

            status.update("Constructing header...")
            header.construct_header(
                assets_path=constants.ASSETS_PATH,
                canvas=c,
                output_path=dest_file_name,
                interview_metadata=interview_metadata,
            )

            status.update("Constructing video section...")
            video.construct_am_report(
                canvas=c,
                interview_name=interview_name,
                start_time=start_timedelta,
                end_time=end_timedelta,
                frame_frequency=frame_frequency,
                interview_metadata=interview_metadata,
                config_file=config_file,
                min_labels=min_labels,
                heatmap_vid_pose_pt_path=Path(heatmap_vid_pose_gaze_pt.name),
                heatmap_vid_fau_pt_path=Path(heatmap_vid_fau_pt.name),
                heatmap_vid_pose_int_path=Path(heatmap_vid_pose_gaze_int.name),
                heatmap_vid_fau_int_path=Path(heatmap_vid_fau_int.name),
                corr_vid_pt_path=Path(correlation_matrix_pt_path.name),
                corr_vid_int_path=Path(correlation_matrix_int_path.name),
                assets_path=constants.ASSETS_PATH,
                headpose_labels=constants.HEADPOSE_FEATURES,
                gaze_labels=constants.GAZE_FEATURES,
                au_labels=constants.AU_LABELS,
                pose_cols=constants.HEADPOSE_COLS,
                gaze_cols=constants.GAZE_COLS,
                au_cols=constants.AU_COLS,
                ticks_config=constants.ticks_config,
                cluster_bars_config=constants.cluster_bars_config,
                data_path=constants.DATA_PATH,
                deidentified=anonymize,
            )

            status.update("Writing metadata...")
            common.print_visit_and_participant_metadata(
                canvas=c,
                interview_metadata=interview_metadata,
                config_file=config_file,
                data_type="video",
            )

            common.print_page_numbers(
                canvas=c,
                current_page=page_number,
                total_pages=num_pages,
            )

            # Save the page
            c.showPage()
            page_number += 1
            start_timestap = end_timestamp

            for temp_file in temp_files:
                temp_file.close()

        for temp_file in temp_files_common:
            temp_file.close()

        console.log("Saving report...")
        c.save()
