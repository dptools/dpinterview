"""
ClusterBarsConfig Class
"""


class ClusterBarsConfig:
    """
    Represents the configuration for the cluster bars.

    Attributes:
        cluster_bars_space (float): The space between the cluster bars.
        cluster_bars_width (float): The width of the cluster bars.
    """

    def __init__(
        self,
        cluster_bars_space: float,
        cluster_bars_width: float,
    ):
        self.cluster_bars_space = cluster_bars_space
        self.cluster_bars_width = cluster_bars_width
