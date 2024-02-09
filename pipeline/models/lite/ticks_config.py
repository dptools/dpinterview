"""
TicksConfig class
"""


class TicksConfig:
    """
    A class representing the configuration for ticks on a plot.

    Attributes:
        min_label_offset_ticks_y (float): The minimum offset for the y-axis tick labels.
        ticks_offset_y (float): The offset for the y-axis ticks.
        tick_height (float): The height of the ticks.
        ticks_large_spacing (float): The spacing between large ticks.
        ticks_small_spacing (float): The spacing between small ticks.
    """

    def __init__(
        self,
        min_label_offset_ticks_y: float,
        ticks_offset_y: float,
        tick_height: float,
        ticks_large_spacing: float,
        ticks_small_spacing: float,
    ):
        self.min_label_offset_ticks_y = min_label_offset_ticks_y
        self.ticks_offset_y = ticks_offset_y
        self.tick_height = tick_height
        self.ticks_large_spacing = ticks_large_spacing
        self.ticks_small_spacing = ticks_small_spacing
