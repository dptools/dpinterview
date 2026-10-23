"""Acoustic features from .wav recordings via opensmile's eGeMAPSv02 set.

For each recording, writes the 25 low-level descriptors as a per-frame CSV,
into a tree that mirrors the input tree. See <https://audeering.github.io/opensmile-python/>.
"""

import sys
from pathlib import Path

file = Path(__file__).resolve()
parent = file.parent
ROOT = None
for parent in file.parents:
    if parent.name == 'dpinterview':
        ROOT = parent
sys.path.append(str(ROOT))

# remove current directory from path
try:
    sys.path.remove(str(parent))
except ValueError:
    pass

import argparse

from pipeline.core.acoustics import AcousticRecording


def process_tree(input_root: Path, output_root: Path) -> None:
    """Extract and save features for every .wav under input_root.

    Args:
        input_root: Directory searched recursively for .wav files.
        output_root: Root of the mirrored output tree.
    """
    for audio_path in sorted(input_root.rglob('*.wav')):
        output_dir = output_root / audio_path.parent.relative_to(input_root)
        AcousticRecording(audio_path).save(output_dir)
        print(f'{audio_path} -> {output_dir}')


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument('input_root', type=Path)
    parser.add_argument('output_root', type=Path)
    args = parser.parse_args()
    process_tree(args.input_root, args.output_root)


if __name__ == '__main__':
    main()
