"""
HeatmapConfig Class
"""


class HeatmapConfig:
    """
    Configuration class for heatmaps.

    Args:
        linewidth (float): The width of the lines separating the bins in the heatmap.
        gap_size (float): The size of the gap between the bins in the heatmap.
        bins_per_v_gap (int): The number of bins to include in each vertical gap in the heatmap.
    """

    def __init__(self, linewidth: float, gap_size: float, bins_per_v_gap: int):
        self.linewidth = linewidth
        self.gap_size = gap_size
        self.bins_per_v_gap = bins_per_v_gap
