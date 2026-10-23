"""
eGeMAPSv02 acoustic feature extraction via opensmile.

See <https://audeering.github.io/opensmile-python/>.
"""

from pathlib import Path

import opensmile
import pandas as pd

# Resampled to 16 kHz mono so every site's spectral features come from the same input rate;
# channels=None selects all channels for the mixdown (opensmile defaults to the first only).
_LLD_SMILE = opensmile.Smile(
    feature_set=opensmile.FeatureSet.eGeMAPSv02,
    feature_level=opensmile.FeatureLevel.LowLevelDescriptors,
    sampling_rate=16000,
    resample=True,
    channels=None,
    mixdown=True,
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

    def save(self, output_dir: Path, name: str) -> Path:
        """Write `<name>_lld.csv`, indexed by frame start in seconds.

        Args:
            output_dir: Directory to write into; created if absent.
            name: Stem shared by this recording's output files.

        Returns:
            The CSV path written.
        """
        output_dir.mkdir(parents=True, exist_ok=True)
        lld_path = output_dir / f'{name}_lld.csv'
        lld = self.lld.copy()
        lld.index = lld.index.total_seconds()
        lld.to_csv(lld_path, index_label='start_s')
        return lld_path
