"""
Hard-failure QC gating acoustic extraction: checks only for audio no extraction could use.
"""

# TODO: consider merging with the `qc_audio_file` variants in the AMPSCZ transcription QC runners,
# which apply speech-quality thresholds tuned for combined audio.

from dataclasses import dataclass
from typing import Literal

import numpy as np

from pipeline.core.acoustics import SAMPLING_RATE

MIN_DURATION_S = 1.0  # same floor as the transcription QC

FailReason = Literal['unreadable', 'too_short', 'all_zero_peak']


@dataclass(frozen=True)
class SignalQc:
    """Hard-failure verdict on one recording.

    Attributes:
        duration_s: Signal length in seconds; None if the file could not be decoded.
        peak_amplitude: Largest absolute sample; None if the file could not be decoded.
        fail_reasons: The checks that failed; empty if the recording passed.
    """

    duration_s: float | None
    peak_amplitude: float | None
    fail_reasons: tuple[FailReason, ...]

    @property
    def passed(self) -> bool:
        """Whether every check passed."""
        return not self.fail_reasons

    @classmethod
    def from_signal(cls, signal: np.ndarray) -> 'SignalQc':
        """Check a decoded signal.

        Args:
            signal: 16 kHz mono float samples, as returned by `acoustics.load_signal`.

        Returns:
            The verdict.
        """
        duration_s = signal.size / SAMPLING_RATE
        peak_amplitude = float(np.max(np.abs(signal))) if signal.size else 0.0

        fail_reasons: list[FailReason] = []
        if duration_s < MIN_DURATION_S:
            fail_reasons.append('too_short')
        if peak_amplitude <= 0.0:
            fail_reasons.append('all_zero_peak')

        return cls(
            duration_s=duration_s,
            peak_amplitude=peak_amplitude,
            fail_reasons=tuple(fail_reasons),
        )

    @classmethod
    def unreadable(cls) -> 'SignalQc':
        """Verdict for a file that could not be decoded.

        Returns:
            A failed verdict with no measurements.
        """
        return cls(duration_s=None, peak_amplitude=None, fail_reasons=('unreadable',))
