"""
Has all the constants used in the pipeline.
"""

from pathlib import Path

from pipeline.models.lite.heatmap_config import HeatmapConfig
from pipeline.models.lite.ticks_config import TicksConfig
from pipeline.models.lite.cluster_bar_config import ClusterBarsConfig

# Paths
REPO_ROOT = Path(__file__).parent.parent
ASSETS_PATH = REPO_ROOT / "pipeline" / "assets"
DATA_PATH = REPO_ROOT / "data"
FAU_METRICS_PT_CACHE = DATA_PATH / "metrics_cache_subject.csv"
FAU_METRICS_INT_CACHE = DATA_PATH / "metrics_cache_interviewer.csv"

# Numbers
NUM_FRAMES = 15

# Features
HEADPOSE_T_FEATURES = ["R(-)toL(+)", "U(-)toD(+)", "F(-)toB(+)"]
HEADPOSE_T_COLS = ["pose_Tx", "pose_Ty", "pose_Tz"]

HEADPOSE_R_FEATURES = ["Pitch[X]", "Yaw[Y]", "Roll[Z]"]
HEADPOSE_R_COLS = ["pose_Rx", "pose_Ry", "pose_Rz"]

GAZE_FEATURES = ["R(-)toL(+)", "U(-)toD(+)"]
GAZE_COLS = ["gaze_angle_x", "gaze_angle_y"]

HEADPOSE_FEATURES = HEADPOSE_T_FEATURES + HEADPOSE_R_FEATURES
HEADPOSE_COLS = HEADPOSE_T_COLS + HEADPOSE_R_COLS

POSE_COLS = HEADPOSE_COLS + GAZE_COLS

AU_COLS = [
    "AU07_r",
    "AU04_r",
    "AU09_r",
    "AU23_r",
    "AU17_r",
    "AU14_r",
    "AU10_r",
    "AU12_r",
    "AU06_r",
    "AU20_r",
    "AU15_r",
    "AU45_r",
    "AU26_r",
    "AU25_r",
    "AU02_r",
    "AU01_r",
    "AU05_r",
]

AU_LABELS = [
    "AU07",
    "AU04",
    "AU09",
    "AU23",
    "AU17",
    "AU14",
    "AU10",
    "AU12",
    "AU06",
    "AU20",
    "AU15",
    "AU45",
    "AU26",
    "AU25",
    "AU02",
    "AU01",
    "AU05",
]

# Headers
HEATMAP_VID_POSE_HEADER = "Head Pose"
HEATMAP_VID_GAZE_HEADER = "Gaze"
HEATMAP_VID_FAUS_HEADER = "Facial Action Units (AUs)"

HEATMAP_WIDTH = 503.949

ticks_small_spacing = HEATMAP_WIDTH / float(30)
ticks_large_spacing = HEATMAP_WIDTH / float(6) - 0.5

ticks_config = TicksConfig(
    min_label_offset_ticks_y=10,
    ticks_offset_y=2,
    tick_height=3.296,
    ticks_large_spacing=ticks_large_spacing,
    ticks_small_spacing=ticks_small_spacing,
)

cluster_bars_config = ClusterBarsConfig(cluster_bars_space=0.6, cluster_bars_width=2.6)

heatmap_config = HeatmapConfig(
    linewidth=0.5,
    gap_size=6,
    bins_per_v_gap=10,
)
