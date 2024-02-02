"""
Plot the correlation matrix
"""

from pathlib import Path
from typing import List

import matplotlib as mpl
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns

from pipeline import data
from pipeline.helpers import dpdash
from pipeline.models.interview_roles import InterviewRole
from pipeline.models.lite.heatmap_config import HeatmapConfig


def combine_matrices(df_bottom: pd.DataFrame, df_top: pd.DataFrame) -> np.ndarray:
    """
    Combines two matrices by replacing the upper triangular elements of the first matrix
    with the corresponding elements from the second matrix, while keeping the lower triangular
    elements unchanged. The diagonal elements are set to NaN.

    Args:
        df_bottom (pd.DataFrame): The first matrix.
        df_top (pd.DataFrame): The second matrix.

    Returns:
        np.ndarray: The combined matrix.
    """
    matrix = np.zeros(df_top.shape)

    for i in range(df_top.shape[0]):
        for j in range(df_top.shape[1]):
            if i < j:
                matrix[i, j] = df_top.iloc[i, j]
            elif i > j:
                matrix[i, j] = df_bottom.iloc[i, j]

    # matrix = matrix / matrix.max()

    # Add the diagonal
    for i in range(matrix.shape[0]):
        # set the diagonal to the NaN
        matrix[i, i] = np.nan

    return matrix


def plot_correrlation_matrix(
    df: pd.DataFrame,
    output_path: Path,
    heatmap_config: HeatmapConfig,
    gap_idx: List[int],
    cmap: str = "BrBG",
    figsize: tuple = (7, 7),
):
    """
    Plots a correlation matrix.

    Args:
        df (pd.DataFrame): The correlation matrix.
        output_path (Path): The path to save the plot.
        heatmap_config (HeatmapConfig): The heatmap configuration.
        gap_idx (List[int]): The indices of the gaps in the correlation matrix.
        cmap (str, optional): The color map to use. Defaults to "BrBG".
        figsize (tuple, optional): The figure size. Defaults to (7, 7).

    Returns:
        None
    """
    plt.figure(figsize=figsize)

    mpl_cmap: mpl.colors.Colormap = mpl.colormaps[cmap]  # type: ignore
    mpl_cmap.set_under("gray")
    mpl_cmap.set_over("gray")

    # Show grey boxes for NaN or no data
    mpl_cmap.set_bad("gray")

    ax = sns.heatmap(
        df,
        annot=False,
        cmap=mpl_cmap,
        cbar=False,
        square=True,
        linewidths=heatmap_config.linewidth,
        vmin=-1,
        vmax=1,
    )

    # disable xand y labels
    ax.xaxis.set_visible(False)
    ax.yaxis.set_visible(False)

    for i in gap_idx:
        ax.axhline(i, color="white", lw=heatmap_config.gap_size)
        ax.axvline(i, color="white", lw=heatmap_config.gap_size)

    # plot a line over the diagonal
    ax.plot([0, 17], [0, 17], color="black", lw=2)

    # Save the plot
    plt.savefig(output_path, bbox_inches="tight", pad_inches=0)


def generate_correlation_matric(
    interview_name: str,
    role: InterviewRole,
    heatmap_config: HeatmapConfig,
    gap_idx: List[int],
    output_path: Path,
    config_file_path: str,
    au_cols: List[str],
    data_path: Path,
) -> None:
    """
    Generates a correlation matrix for the given interview, and saves the plot to the specified
    output path.

    Args:
        interview_name (str): The name of the interview.
        role (InterviewRole): The role of the primary person in the video.
        heatmap_config (HeatmapConfig): The heatmap configuration.
        gap_idx (List[int]): The indices of the gaps in the correlation matrix.
        output_path (Path): The path to save the plot.
        config_file_path (str): The path to the configuration file.
        au_cols (List[str]): The columns to use for the correlation matrix.
        data_path (Path): The path to the data directory.

    Returns:
        None
    """
    dpdash_dict = dpdash.parse_dpdash_name(interview_name)
    subject_id = dpdash_dict["subject"]
    study_id = dpdash_dict["study"]

    of_fau_session = data.fetch_openface_features(
        interview_name=interview_name,
        subject_id=subject_id,
        study_id=study_id,
        role=role,
        cols=au_cols,
        config_file=config_file_path,
    )

    match role:
        case InterviewRole.SUBJECT:
            of_fau_dist = data.fetch_openface_subject_distribution(
                subject_id=subject_id,
                cols=au_cols,
                config_file=config_file_path,
            )
        case InterviewRole.INTERVIEWER:
            of_int_fau_dist_path = data_path / "correlation_matrix_int.csv"
            if not of_int_fau_dist_path.exists():
                raise FileNotFoundError(f"File not found: {of_int_fau_dist_path}")
            of_fau_dist = pd.read_csv(of_int_fau_dist_path)
        case _:
            raise ValueError(f"Invalid role: {role}")

    corr_matrix_session = of_fau_session.corr(method="pearson")
    corr_matrix_dist = of_fau_dist.corr(method="pearson")

    matrix = combine_matrices(df_top=corr_matrix_dist, df_bottom=corr_matrix_session)

    plot_correrlation_matrix(
        df=matrix,  # type: ignore
        output_path=output_path,
        heatmap_config=heatmap_config,
        gap_idx=gap_idx,
    )
