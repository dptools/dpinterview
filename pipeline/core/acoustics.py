"""
eGeMAPSv02 acoustic feature extraction via opensmile.

See <https://audeering.github.io/opensmile-python/>.
"""

import json
from pathlib import Path

import opensmile
import pandas as pd

_LLD_SMILE = opensmile.Smile(
    feature_set=opensmile.FeatureSet.eGeMAPSv02,
    feature_level=opensmile.FeatureLevel.LowLevelDescriptors,
)
_FUNCTIONALS_SMILE = opensmile.Smile(
    feature_set=opensmile.FeatureSet.eGeMAPSv02,
    feature_level=opensmile.FeatureLevel.Functionals,
)


class AcousticRecording:
    """One .wav file with its eGeMAPSv02 LLDs and functionals."""

    def __init__(self, audio_path: Path) -> None:
        """Run both opensmile passes on the file.

        Args:
            audio_path: Path to the .wav file.
        """
        self.audio_path = audio_path
        # Frame start time is the index; window end is implied by the 10 ms hop.
        self.lld: pd.DataFrame = (
            _LLD_SMILE.process_file(str(audio_path)).droplevel(['file', 'end'])
        )
        functionals_row = _FUNCTIONALS_SMILE.process_file(str(audio_path)).iloc[0]
        self.functionals: dict[str, float] = {
            name: float(functionals_row[name]) for name in _FUNCTIONALS_SMILE.feature_names
        }

    def save(self, output_dir: Path) -> None:
        """Write `<stem>_lld.csv` and `<stem>_summary.json` into output_dir.

        Args:
            output_dir: Directory to write into; created if absent.
        """
        output_dir.mkdir(parents=True, exist_ok=True)
        stem = self.audio_path.stem
        lld = self.lld.copy()
        lld.index = lld.index.total_seconds()
        lld.to_csv(output_dir / f'{stem}_lld.csv', index_label='start_s')
        with open(output_dir / f'{stem}_summary.json', 'w') as f:
            json.dump(self.functionals, f, indent=2)
