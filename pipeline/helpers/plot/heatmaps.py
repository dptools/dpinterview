"""
Plotting helper functions for creating heatmaps.
"""

from typing import List

import matplotlib as mpl
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

from pipeline.models.lite.heatmap_config import HeatmapConfig


def make_heatmap(
    df: pd.DataFrame,
    num_bins: int,
    cols: List[str],
    output_path: str,
    heatmap_config: HeatmapConfig,
    figsize=(10, 8),
):
    """
    Create a heatmap based on the given dataframe and configuration.

    Args:
        df (pd.DataFrame): The input dataframe.
        num_bins (int): The number of bins.
        cols (List[str]): The columns to include in the heatmap.
        output_path (str): The path to save the heatmap image.
        heatmap_config (HeatmapConfig): The configuration for the heatmap.
        figsize (tuple, optional): The size of the figure. Defaults to (10, 8).
    """

    # Create a new dataframe for Head Pose Heatmap data
    df_heatmap = pd.DataFrame(columns=["bin"] + cols)

    # Calculate the mean of each bin
    for i in range(num_bins):
        df_bin = df[df["bin"] == i]
        df_heatmap.loc[i] = [i] + [df_bin[col].mean() for col in cols]  # type: ignore

    # Normalize the data
    df_heatmap_norm = df_heatmap.copy()
    df_heatmap_norm[cols] = df_heatmap_norm[cols].apply(
        lambda x: (x - x.min()) / (x.max() - x.min())
    )

    # Setup Color Map
    cmap: mpl.colors.Colormap = mpl.colormaps["PRGn"]  # type: ignore
    cmap.set_under("gray")
    cmap.set_over("gray")
    cmap.set_bad("gray")

    # Create a heatmap using the normalized data
    plt.figure(figsize=figsize)
    ax = sns.heatmap(
        df_heatmap_norm[cols].T,
        cmap=cmap,
        cbar=False,
        square=True,
        linewidths=heatmap_config.linewidth,  # type: ignore
    )

    # No Labels
    plt.xticks([])
    plt.yticks([])

    # add space between cells
    for i in range(num_bins):
        if i % heatmap_config.bins_per_v_gap == 0:
            ax.axvline(i, color="white", lw=heatmap_config.gap_size)

    # Make each cell square
    plt.gca().set_aspect("equal", adjustable="box")

    # Save the figure
    plt.savefig(output_path, bbox_inches="tight", pad_inches=0)

    # close the figure
    plt.close()


def make_standard_deviation_heatmap(
    df: pd.DataFrame,
    fau_avgs: pd.Series,
    fau_stds: pd.Series,
    num_bins: int,
    features: List[str],
    cols: List[str],
    output_path: str,
    heatmap_config: HeatmapConfig,
    h_gap_idx: List[int],
    figsize=(20, 7),
    normalize=True,
):
    """
    Creates a standard deviation heatmap based on the given data.

    Args:
        df (pd.DataFrame): The input DataFrame containing the data.
        fau_avgs (pd.Series): The average values for each feature.
        fau_stds (pd.Series): The standard deviation values for each feature.
        num_bins (int): The number of bins for the heatmap.
        features (List[str]): The list of feature names.
        cols (List[str]): The list of column names in the DataFrame.
        output_path (str): The path to save the generated heatmap image.
        heatmap_config (HeatmapConfig): The configuration object for the heatmap.
        h_gap_idx (List[int]): The list of indices to add horizontal gaps in the heatmap.
        figsize (tuple, optional): The size of the figure. Defaults to (20, 7).
        normalize (bool, optional): Flag indicating whether to normalize the data. Defaults to True.
    """
    df_heatmap = pd.DataFrame(columns=["bin"] + features)

    for i in range(num_bins):
        df_bin = df[df["bin"] == i]
        for col, feature in zip(cols, features):
            df_heatmap.loc[i, "bin"] = i
            df_heatmap.loc[i, feature] = df_bin[col].mean()

            # Scale each bin to be with 3 standard deviations of the distribution
            dist_mean = fau_avgs[col]
            dist_std = fau_stds[col]

            max_val = dist_mean + (3 * dist_std)
            min_val = dist_mean - (3 * dist_std)

            if df_heatmap.loc[i, feature] > max_val:
                df_heatmap.loc[i, feature] = max_val
            elif df_heatmap.loc[i, feature] < min_val:
                df_heatmap.loc[i, feature] = min_val
            else:
                current_value = df_heatmap.loc[i, feature]
                new_value = ((current_value - min_val) / (max_val - min_val)) * max_val
                df_heatmap.loc[i, feature] = new_value

    # Normalize the data if normalize is True
    if normalize:
        for col, feature in zip(cols, features):
            dist_mean = fau_avgs[col]
            dist_std = fau_stds[col]

            max_val = dist_mean + (3 * dist_std)
            min_val = dist_mean - (3 * dist_std)

            df_heatmap[feature] = df_heatmap[feature].apply(
                lambda x: ((x - min_val) / (max_val - min_val))
            )

    # Setup Color Map
    cmap: mpl.colors.Colormap = mpl.colormaps["bwr"]  # type: ignore
    # cmap = cm.get_cmap('bwr')   # Deprecated
    cmap.set_under("gray")
    cmap.set_over("gray")

    # Show grey boxes for NaN or no data
    cmap.set_bad("gray")

    # Create a heatmap using the data, matplotlib
    plt.figure(figsize=figsize)
    ax = sns.heatmap(
        df_heatmap[features].T.astype(float),
        cmap=cmap,
        cbar=False,
        square=True,
        linewidths=heatmap_config.linewidth,  # type: ignore
    )

    # Hide all labels
    plt.xticks([])
    plt.yticks([])

    # add space between cells
    for i in range(num_bins):
        if i % heatmap_config.bins_per_v_gap == 0:
            ax.axvline(i, color="white", lw=heatmap_config.gap_size)

    # add horizontal lines to separate features
    for i in h_gap_idx:
        ax.axhline(i, color="white", lw=heatmap_config.gap_size)

    # Save the figure
    plt.savefig(output_path, bbox_inches="tight", pad_inches=0)

    # close the figure
    plt.close()
