#!/usr/bin/env python
"""
Convert Audio Journals to a standardized (WAV) format.
QC the audio files to pass basic checks.
"""

import sys
from pathlib import Path

file = Path(__file__).resolve()
parent = file.parent
ROOT = None
for parent in file.parents:
    if parent.name == "dpinterview":
        ROOT = parent
sys.path.append(str(ROOT))

# remove current directory from path
try:
    sys.path.remove(str(parent))
except ValueError:
    pass

import argparse
import logging
from datetime import datetime
from typing import Any, Dict, Optional, Tuple, List

import librosa
import numpy as np
import soundfile as sf
import webrtcvad
from rich.logging import RichHandler

from pipeline import orchestrator
from pipeline.helpers import cli, db, ffmpeg, utils
from pipeline.helpers.timer import Timer
from pipeline.models.files import File
from pipeline.models.transcribeme.audio_qc import AudioQC
from pipeline.models.transcribeme.wav_conversion import WavConversion

MODULE_NAME = "pipeline.runners.ampscz.qc_audio_journals"

logger = logging.getLogger(MODULE_NAME)
logargs = {
    "level": logging.DEBUG,
    # "format": "%(asctime)s - %(process)d - %(name)s - %(levelname)s - %(message)s",
    "format": "%(message)s",
    "handlers": [RichHandler(rich_tracebacks=True)],
}
logging.basicConfig(**logargs)

console = utils.get_console()

noisy_modules: List[str] = ["numba.core.byteflow", "numba.core.interpreter"]
utils.silence_logs(noisy_modules=noisy_modules)


def qc_audio_file(
    file_path: Path,
    ref_rms: float = 2e-5,  # reference RMS for dB SPL (20 μPa) - threshold for human hearing
    # basic checks
    min_duration_seconds: float = 1.0,
    min_spl_db: float = 50.0,  # minimum SPL in dB
    # silence detection
    silence_rms_thresh: float = 0.001,
    max_silence_proportion: float = 0.5,
    # clipping
    clipping_amplitude_thresh: float = 0.99,
    max_clipping_proportion: float = 0.01,
    # DC offset
    max_dc_offset: float = 0.01,
    # SNR
    min_snr_db: float = 20.0,
    # Voice activity
    vad_mode: int = 2,  # 0–3 more aggressive
    min_voice_proportion: float = 0.1,
) -> Tuple[bool, Dict[str, Any], Dict[str, bool]]:
    """
    Minimal QC for wav/flac audio:
    - Level checks (RMS, peak, DC offset)
    - Silence proportion
    - Clipping proportion
    - Rough SNR estimate
    - Voice activity proportion via WebRTC VAD

    Returns
    -------
    passed : bool
        True if all checks pass
    metrics : dict
        Computed metrics for logging/threshold tuning
    fails : dict
        Which rule(s) failed
    """

    if not file_path.exists():
        raise FileNotFoundError(f"Audio file not found: {file_path}")

    # 1) Read (always_2d to get channels)
    y, sr = sf.read(str(file_path), always_2d=True)
    n_channels = y.shape[1]
    # collapse to mono
    y_mono = np.mean(y, axis=1).astype("float32")
    n_samples = len(y_mono)
    duration = n_samples / sr

    # 2) Level metrics
    overall_rms = float(np.sqrt(np.mean(y_mono**2)))
    peak_amplitude = float(np.max(np.abs(y_mono)))
    dc_offset = float(np.abs(np.mean(y_mono)))

    spl_db = 20 * np.log10(overall_rms / ref_rms)
    dbfs = 20 * np.log10(overall_rms)  # dBFS (0dBFS = max digital level)

    # 3) Frame‐level RMS -> silence proportion
    frame_length = 2048
    hop_length = 512
    rms_frames = librosa.feature.rms(
        y=y_mono, frame_length=frame_length, hop_length=hop_length
    )[0]
    silence_proportion = float(np.mean(rms_frames < silence_rms_thresh))

    # 4) Clipping
    clipping_proportion = float(np.mean(np.abs(y_mono) >= clipping_amplitude_thresh))

    # 5) Rough SNR (signal vs. quiet‐frame RMS)
    sorted_rms = np.sort(rms_frames)
    floor_count = max(1, int(0.2 * len(sorted_rms)))
    noise_floor = float(np.median(sorted_rms[:floor_count]))
    noise_floor = max(noise_floor, 1e-9)  # avoid divide‐by‐zero
    snr_db = 20.0 * np.log10(overall_rms / noise_floor)

    # 6) Voice Activity via WebRTC VAD
    # pick a VAD‐friendly sampling rate
    # https://github.com/wiseman/py-webrtcvad/issues/3
    VAD_RATES = (8000, 16000, 32000)
    if sr in VAD_RATES:
        y_vad = y_mono
        vad_sr = sr
    else:
        # choose the closest supported rate (or just hard‐code 16000)
        vad_sr = min(VAD_RATES, key=lambda r: abs(r - sr))
        # resample float32 -> float32
        y_vad = librosa.resample(y_mono, orig_sr=sr, target_sr=vad_sr)

    # now pack into 16‐bit PCM
    pcm16 = (y_vad * 32767).astype(np.int16).tobytes()

    # run VAD on y_vad @ vad_sr
    vad = webrtcvad.Vad(vad_mode)
    frame_ms = 30
    frame_bytes = int(vad_sr * frame_ms / 1000) * 2  # 2 bytes per sample
    voiced, total = 0, 0
    for offset in range(0, len(pcm16) - frame_bytes + 1, frame_bytes):
        is_speech = vad.is_speech(
            pcm16[offset: offset + frame_bytes], sample_rate=vad_sr
        )
        voiced += 1 if is_speech else 0
        total += 1

    voice_proportion = voiced / max(1, total)

    # 7) Compile metrics & fails
    metrics: Dict[str, Any] = {
        "sample_rate": sr,
        "channels": n_channels,
        "spl_db": spl_db,
        "dbfs": dbfs,
        "duration_seconds": duration,
        "overall_rms": overall_rms,
        "peak_amplitude": peak_amplitude,
        "dc_offset": dc_offset,
        "silence_proportion": silence_proportion,
        "clipping_proportion": clipping_proportion,
        "snr_db": snr_db,
        "voice_proportion": voice_proportion,
    }

    fails: Dict[str, bool] = {
        "too_short": duration < min_duration_seconds,
        "low_spl": spl_db < min_spl_db,
        "too_much_silence": silence_proportion >= max_silence_proportion,
        "all_zero_peak": peak_amplitude <= 0.0,
        "too_much_clipping": clipping_proportion > max_clipping_proportion,
        "high_dc_offset": dc_offset > max_dc_offset,
        "low_snr": snr_db < min_snr_db,
        "low_voice_activity": voice_proportion < min_voice_proportion,
    }

    # Drop fails that are not true
    fails = {k: v for k, v in fails.items() if v}

    passed = not any(fails.values())
    return passed, metrics, fails


def get_file_to_process(
    config_file: Path, study_id: str
) -> Optional[Tuple[Path, str, str, str]]:
    """
    Get the next file to process from the database.

    This function queries the database for audio journals that have not been
    transcribed yet.

    Args:
        config_file (Path): Path to the config file.
        study_id (str): Study ID to filter the audio journals.

    Returns:
        Optional[Tuple[Path, str, str, str]]: A tuple containing the journal path,
            journal name, subject ID, and study ID. Returns None if no files are found.
    """
    query = f"""
    SELECT aj_path, aj_name, subject_id, study_id
    FROM audio_journals
    LEFT JOIN transcript_files ON audio_journals.aj_name = transcript_files.identifier_name
    WHERE transcript_files.identifier_name is NULL AND  -- not yet transcribed
        study_id = '{study_id}' AND
        aj_path NOT IN (  -- exclude files already in QC or conversion
            SELECT wc_source_path
            FROM transcribeme.audio_qc
            LEFT JOIN transcribeme.wav_conversion
                ON transcribeme.wav_conversion.wc_destination_path =
                    transcribeme.audio_qc.aqc_source_path
        ) AND
        aj_path NOT IN (
            SELECT wc_source_path
            FROM transcribeme.wav_conversion
        )
    ORDER BY aj_name
    LIMIT 1
    """

    result_df = db.execute_sql(
        config_file=config_file,
        query=query,
    )

    if result_df.empty:
        return None

    journal_path = Path(result_df.iloc[0]["aj_path"])
    journal_name = result_df.iloc[0]["aj_name"]
    subject_id = result_df.iloc[0]["subject_id"]
    study_id = result_df.iloc[0]["study_id"]

    return journal_path, journal_name, subject_id, study_id


def get_subject_journals_root(
    config_file: Path, subject_id: str, study_id: str
) -> Path:
    """
    Get the root directory for the subject's journals.

    Args:
        config_file (Path): Path to the config file.
        subject_id (str): Subject ID.
        study_id (str): Study ID.

    Returns:
        Path: Path to the subject's journals root directory.
    """
    data_root = orchestrator.get_data_root(config_file=config_file, enforce_real=True)
    subject_root = data_root / "PROTECTED" / study_id / "processed" / subject_id
    subject_journals_root = subject_root / "phone" / "audio_journals"

    return subject_journals_root


def construct_temp_wav_file_path(
    subject_journals_root: Path, journal_name: str
) -> Path:
    """
    Construct the path for the pending WAV file.

    Files are held here, while waiting for QC results

    Args:
        subject_journals_root (Path): Path to the subject's journals root directory.
        journal_name (str): Name of the journal.

    Returns:
        Path: Path to the pending WAV file.
    """
    wav_file_path = subject_journals_root / "temp_audio" / f"{journal_name}.wav"
    if not wav_file_path.parent.exists():
        wav_file_path.parent.mkdir(parents=True, exist_ok=True)
    return wav_file_path


def log_qc_results(
    source_path: Path,
    passed: bool,
    metrics: Dict[str, Any],
    fails: Dict[str, bool],
    qc_duration_s: float,
    wav_conversion: WavConversion,
    config_file: Path,
) -> None:
    """
    Log the results of the audio QC.

    Args:
        source_path (Path): Path to the audio file.
        passed (bool): Whether the QC passed.
        metrics (Dict[str, Any]): Metrics collected during the QC.
        fails (Dict[str, bool]): Reasons for failure if the QC did not pass.
        config_file (Path): Path to the config file.
    """
    qc = AudioQC(
        aqc_source_path=source_path,
        aqc_passed=passed,
        aqc_metrics=metrics,
        aqc_fail_reasons=fails,
        aqc_duration_s=qc_duration_s,
        aqc_timestamp=datetime.now(),
    )

    wav_file = File(file_path=wav_conversion.wc_destination_path)

    db.execute_queries(
        config_file=config_file,
        queries=[
            wav_file.to_sql(),
            wav_conversion.to_sql(),
            qc.to_sql(),
        ],
    )


def handle_post_qc(wav_conversion: WavConversion, qc_pass: bool) -> WavConversion:
    """
    Handle QC results:
    fail - moving the file to a failed directory.
    pass - moving the file to the pending path.

    Args:
        wav_conversion (WavConversion): The WavConversion object containing the file paths.
    """
    dest_dir = None
    if qc_pass:
        passed_dir = wav_conversion.wc_source_path.parent / "pending_audio"
        if not passed_dir.exists():
            passed_dir.mkdir(parents=True, exist_ok=True)
        dest_dir = passed_dir
    else:
        failed_dir = wav_conversion.wc_source_path.parent / "rejected_audio"
        if not failed_dir.exists():
            failed_dir.mkdir(parents=True, exist_ok=True)
        dest_dir = failed_dir

    # Move the file to the appropriate directory
    new_path = dest_dir / wav_conversion.wc_destination_path.name
    wav_conversion.wc_destination_path.rename(new_path)

    logger.info(
        f"Moved {wav_conversion.wc_destination_path} to {new_path}",
        extra={"markup": True},
    )

    # Update the WavConversion object with the new path
    wav_conversion.wc_destination_path = new_path
    return wav_conversion


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog=MODULE_NAME, description="Run Quick QC on video files."
    )
    parser.add_argument(
        "-c", "--config", type=str, help="Path to the config file.", required=False
    )

    args = parser.parse_args()

    # Check if parseer has config file
    if args.config:
        config_file = Path(args.config).resolve()
        if not config_file.exists():
            logger.error(f"Error: Config file '{config_file}' does not exist.")
            sys.exit(1)
    else:
        if cli.confirm_action("Using default config file."):
            config_file = utils.get_config_file_path()
        else:
            sys.exit(1)

    utils.configure_logging(
        config_file=config_file, module_name=MODULE_NAME, logger=logger
    )

    console.rule(f"[bold red]{MODULE_NAME}")
    logger.info(f"Using config file: {config_file}")

    config_params = utils.config(config_file, section="general")

    # studies = orchestrator.get_studies(config_file=config_file)
    studies = [
        "PronetYA",
        "PronetBI",
        "PronetCA",
        "PronetCM",
        "PronetGA",
        "PronetHA",
        "PronetMA",
        "PronetMT",
        "PronetMU",
        "PronetNC",
        "PronetNN",
        "PronetPA",
        "PronetPI",
        "PronetPV",
        "PronetSD",
        "PronetSF",
        "PronetSH",
        "PronetSL",
        "PronetTE",
        "PronetUR",
        "PronetWU",
    ]
    held_studies = [
        "PronetIR",
        "PronetKC",
        "PronetLA",
        "PronetNL",
        "PronetOR",
        "PronetSI",
    ]

    COUNTER = 0

    logger.info("Startig audio_qc loop...", extra={"markup": True})
    study_id = studies[0]
    logger.info(f"Using study: {study_id}")

    while True:
        file_to_process = get_file_to_process(
            config_file=config_file, study_id=study_id
        )

        if file_to_process is None:
            if study_id == studies[-1]:
                # Log if any files were processed
                if COUNTER > 0:
                    orchestrator.log(
                        config_file=config_file,
                        module_name=MODULE_NAME,
                        message=f"QC'd {COUNTER} audio journals.",
                    )
                    COUNTER = 0

                # Exit if all studies are done
                logger.info(
                    "No more audio journals to process. Exiting.",
                    extra={"markup": True},
                )
                sys.exit(0)
            else:
                study_id = studies[studies.index(study_id) + 1]
                logger.info(f"Switching to study: {study_id}", extra={"markup": True})
                continue

        COUNTER += 1
        journal_path, journal_name, subject_id, study_id = file_to_process
        logger.info(
            f"Handling Journal: {journal_path}",
            extra={"markup": True},
        )

        subject_journals_root = get_subject_journals_root(
            config_file=config_file, subject_id=subject_id, study_id=study_id
        )

        # Convert audio to wav before uploading
        pending_wav_file_path = construct_temp_wav_file_path(
            subject_journals_root=subject_journals_root,
            journal_name=journal_name,
        )

        logger.info(
            f"Converting {journal_path} to {pending_wav_file_path}",
        )
        with Timer() as timer:
            ffmpeg.convert_audio(source=journal_path, target=pending_wav_file_path)

        wav_conversion = WavConversion(
            wc_source_path=journal_path,
            wc_destination_path=pending_wav_file_path,
            wc_duration_s=timer.duration,  # type: ignore
        )

        # QC the audio file
        with Timer() as timer:
            passed, metrics, fails = qc_audio_file(file_path=pending_wav_file_path)
        if passed:
            logger.info(
                f"Audio QC passed for {journal_name}",
                extra={"markup": True},
            )
        else:
            logger.warning(
                f"Audio QC failed for {journal_name}: {fails}",
                extra={"markup": True},
            )

        wav_conversion = handle_post_qc(wav_conversion=wav_conversion, qc_pass=passed)

        # Log the QC results
        log_qc_results(
            source_path=wav_conversion.wc_destination_path,
            passed=passed,
            metrics=metrics,
            fails=fails,
            wav_conversion=wav_conversion,
            config_file=config_file,
            qc_duration_s=int(timer.duration),  # type: ignore
        )
        logger.info(
            f"Logged QC results for {journal_name}",
            extra={"markup": True},
        )
