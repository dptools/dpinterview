"""
Contains constants used in the disfluencies module.
"""

from typing import Dict, List, Set

supported_languages: Set[str] = {
    "zh",  # Chinese
    "da",  # Danish
    "de",  # German
    # "nl",  # Dutch
    "en",  # English
    "es",  # Spanish
    "fr",  # French
    "it",  # Italian
    "ko",  # Korean
}

zh_punctuations_mapping: Dict[str, str] = {
    " ": " ",
    "，": ",",
    "。": ".",
    "？": "?",
    "！": "!",
    "；": ";",
    "：": ":",
    "、": ",",
}

non_verbal_fillers: Dict[str, List[str]] = {
    "zh": [
        "哦",
        "嗯",
        "嗯嗯",
        "呃",  # Source: TranscribeMe
        "囉",
        "嘅",
        "啦",
        "呢",  # Source: Native Speaker
    ],
    "ko": ["음", "네", "아", "어"],
    "en": [
        "Uh",
        "Ah",
        "Um",
        "hmm",
        "Uh-huh",
        "Mm-hmm",
        "Uh-uh",
        "Hmm-mm",
        "Mm-mm",
        "Huh-uh",
        "Nuh-uh",
    ],
    "de": ["Äh", "Ähm", "Mh", "Ha", "Mm-hmm"],
    "fr": [
        "Ah",
        "Euh",
        "Hmm",
        "Mm-hmm",
        "Uh",
        "Um",
        "Uh-huh",
        "Hum",
        "Nan",
        "Uh-uh",
        "Hmm-mm",
        "Mm-mm",
        "Huh-uh",
        "Nuh-uh",
        "Bah",
        "Eh",
        "Oh",
    ],
    "it": ["Ehm", "Eh", "Uhm", "Boh"],
    "es": [
        "Ah",
        "Eh",
        "Pfff",
        "Hmm",
        "Ajá",
        "Mm-hmm",
        "Um",
        "Aha",
        "Uh-uh",
        "Hmm-mm",
        "Mm-mm",
        "Huh-uh",
        "Nuh-uh",
        "Em",
    ],
}
