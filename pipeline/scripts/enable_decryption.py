#!/usr/bin/env python
"""
Enables decryption of files
"""

import sys
from pathlib import Path

file = Path(__file__).resolve()
parent = file.parent
ROOT = None
for parent in file.parents:
    if parent.name == "av-pipeline-v2":
        ROOT = parent
sys.path.append(str(ROOT))

# remove current directory from path
try:
    sys.path.remove(str(parent))
except ValueError:
    pass

from pipeline.helpers import utils
from pipeline import orchestrator

console = utils.get_console()

if __name__ == "__main__":
    config_file = utils.get_config_file_path()

    console.rule("[green]Enabling Decryption")
    orchestrator.request_decrytion(config_file=config_file, requester="fetch_video")

    console.log("[green]Done!")
