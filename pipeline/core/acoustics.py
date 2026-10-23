"""
eGeMAPSv02 acoustic feature extraction via opensmile.

See <https://audeering.github.io/opensmile-python/>.
"""

from pathlib import Path

import opensmile
import pandas as pd

_LLD_SMILE = opensmile.Smile(
    feature_set=opensmile.FeatureSet.eGeMAPSv02,
    feature_level=opensmile.FeatureLevel.LowLevelDescriptors,
)


class AcousticRecording:
    """One audio file with its eGeMAPSv02 per-frame LLDs."""

    def __init__(self, audio_path: Path) -> None:
        """Run the LLD pass on the file.

        Args:
            audio_path: Path to the audio file.
        """
        self.audio_path = audio_path
        # Frame start time is the index; window end is implied by the 10 ms hop.
        self.lld: pd.DataFrame = (
            _LLD_SMILE.process_file(str(audio_path)).droplevel(['file', 'end'])
        )

    def save(self, output_dir: Path) -> None:
        """Write `<stem>_lld.csv` into output_dir, indexed by frame start in seconds.

        Args:
            output_dir: Directory to write into; created if absent.
        """
        output_dir.mkdir(parents=True, exist_ok=True)
        lld = self.lld.copy()
        lld.index = lld.index.total_seconds()
        lld.to_csv(output_dir / f'{self.audio_path.stem}_lld.csv', index_label='start_s')
