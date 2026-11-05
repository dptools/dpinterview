"""
eGeMAPSv02 acoustic feature extraction via opensmile.

See <https://audeering.github.io/opensmile-python/>.
"""

import json
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import opensmile
import pandas as pd
import soundfile as sf

# Non-public opensmile API, used to read both levels from one run; see verify_single_pass().
from opensmile.core.config import config as smile_config
from opensmile.core.lib import OpenSMILE

from pipeline.helpers import ffmpeg

# Every input is converted to this so all sites' spectral features come from the same rate.
SAMPLING_RATE = 16000

# Public objects: the config file, feature names, and the reference for verify_single_pass().
_LLD_SMILE = opensmile.Smile(
    feature_set=opensmile.FeatureSet.eGeMAPSv02,
    feature_level=opensmile.FeatureLevel.LowLevelDescriptors,
)
_FUNCTIONALS_SMILE = opensmile.Smile(
    feature_set=opensmile.FeatureSet.eGeMAPSv02,
    feature_level=opensmile.FeatureLevel.Functionals,
)


class UnreadableAudioError(Exception):
    """Raised when a source recording cannot be decoded."""


def load_signal(source_path: Path) -> np.ndarray:
    """Decode a recording to a 16 kHz mono signal, via a WAV deleted before returning.

    Args:
        source_path: Any audio file FFmpeg can read.

    Returns:
        Float32 samples in [-1, 1].

    Raises:
        UnreadableAudioError: If FFmpeg cannot decode the file.
    """

    def raise_unreadable() -> None:
        raise UnreadableAudioError(f'FFmpeg could not decode {source_path}')

    with tempfile.TemporaryDirectory(prefix='acoustics-') as temp_dir:
        wav_path = Path(temp_dir) / f'{source_path.stem}.wav'
        ffmpeg.convert_audio(
            source=source_path,
            target=wav_path,
            output_args=['-ar', str(SAMPLING_RATE), '-ac', '1'],
            on_fail=raise_unreadable,
        )
        signal, _ = sf.read(str(wav_path), dtype='float32')
    return signal


def _run_smile(signal: np.ndarray) -> tuple[pd.DataFrame, dict[str, float]]:
    """Run eGeMAPSv02 once on a 16 kHz mono signal, collecting both feature levels.

    Args:
        signal: Float samples in [-1, 1].

    Returns:
        Per-frame LLDs indexed by frame start in seconds, and the functionals by name.
    """
    # Mirrors the options opensmile.Smile builds for itself, with its two-sink config swapped in.
    root = _FUNCTIONALS_SMILE.default_config_root
    options = {
        'source': os.path.join(root, smile_config.EXTERNAL_INPUT_CONFIG),
        'sink': os.path.join(root, smile_config.EXTERNAL_OUTPUT_MULTI_CONFIG),
        'sampleRate': SAMPLING_RATE,
        'nBits': 16,
        'bufferModeRbConf': os.path.join(root, 'shared/BufferModeRb.conf.inc'),
        'frameModeFunctionalsConf': os.path.join(root, 'shared/FrameModeFunctionals.conf.inc'),
    }
    lld_rows: list[np.ndarray] = []
    lld_starts: list[float] = []
    functionals_rows: list[np.ndarray] = []

    smile = OpenSMILE()
    smile.initialize(config_file=_FUNCTIONALS_SMILE.config_path, options=options, loglevel=2)
    try:
        smile.external_sink_set_callback_ex(
            'extsinklld',
            lambda data, meta: (lld_rows.append(data.copy()), lld_starts.append(meta.time)),
        )
        smile.external_sink_set_callback_ex(
            'extsinkfunc',
            lambda data, meta: functionals_rows.append(data.copy()),
        )
        # Same int16 scaling opensmile.Smile applies before writing to the source.
        pcm = (signal * 32768).astype(np.int16)
        smile.external_audio_source_write_data(
            smile_config.EXTERNAL_SOURCE_COMPONENT, pcm.tobytes()
        )
        smile.external_audio_source_set_eoi(smile_config.EXTERNAL_SOURCE_COMPONENT)
        smile.run()
    finally:
        smile.free()

    lld = pd.DataFrame(
        np.vstack(lld_rows),
        index=pd.Index(lld_starts, name='start_s'),
        columns=_LLD_SMILE.feature_names,
    )
    functionals = dict(
        zip(_FUNCTIONALS_SMILE.feature_names, map(float, np.vstack(functionals_rows)[0]))
    )
    return lld, functionals


def verify_single_pass() -> None:
    """Check `_run_smile` against the public two-pass API on a synthetic voiced signal.

    Raises:
        RuntimeError: If any feature value or frame time differs.
    """
    rng = np.random.default_rng(0)
    t = np.arange(3 * SAMPLING_RATE) / SAMPLING_RATE
    f0 = 140 + 20 * np.sin(2 * np.pi * 0.5 * t)  # gliding pitch so the F0 path is exercised
    phase = 2 * np.pi * np.cumsum(f0) / SAMPLING_RATE
    signal = sum(np.sin(k * phase) / k for k in range(1, 8)) * 0.2
    signal = (signal + 0.01 * rng.standard_normal(t.size)).astype(np.float32)

    lld, functionals = _run_smile(signal)
    reference_lld = _LLD_SMILE.process_signal(signal, SAMPLING_RATE).droplevel('end')
    reference_functionals = _FUNCTIONALS_SMILE.process_signal(signal, SAMPLING_RATE).iloc[0]

    matches = (
        np.array_equal(lld.to_numpy(), reference_lld.to_numpy(), equal_nan=True)
        and np.allclose(lld.index, reference_lld.index.total_seconds())
        and np.array_equal(
            np.array(list(functionals.values())),
            reference_functionals.to_numpy(),
            equal_nan=True,
        )
    )
    if not matches:
        raise RuntimeError(
            f'Single-pass extraction no longer matches opensmile {opensmile.__version__}; '
            'see pipeline/core/acoustics.py.'
        )


@dataclass(frozen=True)
class AcousticOutputs:
    """Files written for one recording.

    Attributes:
        lld_path: Per-frame LLD CSV.
        summary_path: Functionals JSON.
    """

    lld_path: Path
    summary_path: Path


class AcousticFeatures:
    """eGeMAPSv02 per-frame LLDs and whole-signal functionals of one single-speaker signal."""

    def __init__(self, signal: np.ndarray) -> None:
        """Extract both feature levels in one opensmile run.

        Args:
            signal: 16 kHz mono float samples, as returned by `load_signal`.
        """
        self.lld, self.functionals = _run_smile(signal)

    def save(self, output_dir: Path, name: str) -> AcousticOutputs:
        """Write `<name>_lld.csv` and `<name>_summary.json`.

        Args:
            output_dir: Directory to write into; created if absent.
            name: Stem shared by this recording's output files.

        Returns:
            The paths written.
        """
        output_dir.mkdir(parents=True, exist_ok=True)
        lld_path = output_dir / f'{name}_lld.csv'
        self.lld.to_csv(lld_path)

        summary_path = output_dir / f'{name}_summary.json'
        with open(summary_path, 'w', encoding='utf-8') as f:
            json.dump(self.functionals, f, indent=2)
        return AcousticOutputs(lld_path=lld_path, summary_path=summary_path)
