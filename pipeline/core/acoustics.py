"""
eGeMAPSv02 acoustic feature extraction via opensmile.

See <https://audeering.github.io/opensmile-python/>.
"""

import json
from dataclasses import dataclass
from pathlib import Path

import opensmile
import pandas as pd

# Resampled to 16 kHz mono so every site's spectral features come from the same input rate;
# channels=None selects all channels for the mixdown (opensmile defaults to the first only).
_SMILE_INPUT = {
    'sampling_rate': 16000,
    'resample': True,
    'channels': None,
    'mixdown': True,
}
_LLD_SMILE = opensmile.Smile(
    feature_set=opensmile.FeatureSet.eGeMAPSv02,
    feature_level=opensmile.FeatureLevel.LowLevelDescriptors,
    **_SMILE_INPUT,
)
_FUNCTIONALS_SMILE = opensmile.Smile(
    feature_set=opensmile.FeatureSet.eGeMAPSv02,
    feature_level=opensmile.FeatureLevel.Functionals,
    **_SMILE_INPUT,
)


@dataclass(frozen=True)
class AcousticOutputs:
    """Files written for one recording.

    Attributes:
        lld_path: Per-frame LLD CSV.
        summary_path: Functionals JSON, if the recording type gets one.
    """

    lld_path: Path
    summary_path: Path | None = None


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

    def save(self, output_dir: Path, name: str) -> AcousticOutputs:
        """Write `<name>_lld.csv`, indexed by frame start in seconds.

        Args:
            output_dir: Directory to write into; created if absent.
            name: Stem shared by this recording's output files.

        Returns:
            The paths written.
        """
        output_dir.mkdir(parents=True, exist_ok=True)
        lld_path = output_dir / f'{name}_lld.csv'
        lld = self.lld.copy()
        lld.index = lld.index.total_seconds()
        lld.to_csv(lld_path, index_label='start_s')
        return AcousticOutputs(lld_path=lld_path)


class SingleSpeakerRecording(AcousticRecording):
    """A recording of one speaker, so whole-file functionals describe that speaker."""

    def __init__(self, audio_path: Path) -> None:
        """Run the LLD and functionals passes on the file.

        Args:
            audio_path: Path to the audio file.
        """
        super().__init__(audio_path)
        functionals_row = _FUNCTIONALS_SMILE.process_file(str(audio_path)).iloc[0]
        self.functionals: dict[str, float] = {
            name: float(functionals_row[name]) for name in _FUNCTIONALS_SMILE.feature_names
        }

    def save(self, output_dir: Path, name: str) -> AcousticOutputs:
        """Write the LLD CSV and `<name>_summary.json`.

        Args:
            output_dir: Directory to write into; created if absent.
            name: Stem shared by this recording's output files.

        Returns:
            The paths written.
        """
        outputs = super().save(output_dir, name)
        summary_path = output_dir / f'{name}_summary.json'
        with open(summary_path, 'w', encoding='utf-8') as f:
            json.dump(self.functionals, f, indent=2)
        return AcousticOutputs(lld_path=outputs.lld_path, summary_path=summary_path)
