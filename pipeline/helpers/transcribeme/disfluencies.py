"""
Funtions to process trnascripts, and extract disfluencies from them.
"""

from pathlib import Path
from typing import List
import re

import pandas as pd

from pipeline.helpers.transcribeme import constants, utils


def preprocess_zh_text(text: str) -> str:
    """
    Preprocesses Chinese text by replacing punctuations with their English equivalents.

    Args:
        text (str): Chinese text

    Returns:
        str: Preprocessed text
    """
    for (
        chinese_punctuation,
        english_punctuation,
    ) in constants.zh_punctuations_mapping.items():
        text = text.replace(chinese_punctuation, english_punctuation)
    return text


def match_fillers(text: str, fillers: List[str], language: str = "en") -> List[str]:
    """
    Counts the number of fillers in a text.

    Args:
        text (str): Text to count fillers

    Returns:
        List[str]: List of fillers found in the text

    Note:
    - The text is converted to lowercase
    - if a filler is found multiple times, it is counted multiple times, and
        has multiple entries in the list
    """
    fillers = [filler.lower() for filler in fillers]
    text = text.lower()

    multi_word_fillers = [filler for filler in fillers if " " in filler]
    fillers = [filler for filler in fillers if " " not in filler]

    found_fillers = []
    if language == "zh":
        # Chinese fillers are not separated by spaces
        # consider each character as a text element (word)
        text = preprocess_zh_text(text)
        text_elements = list(text)
    else:
        text_elements = text.split()
    for text_element in text_elements:
        # Remove any punctuation
        text_element = text_element.strip(".,!?")
        if text_element in fillers:
            found_fillers.append(text_element)

    if len(multi_word_fillers) > 0:
        # Use window of `max_len` to find multi-word fillers
        max_len = max([len(filler.split()) for filler in multi_word_fillers])

        for i in range(len(text_elements) - max_len + 1):
            window = text_elements[i: i + max_len]

            # remove any punctuation
            window = [element.strip(".,!?-") for element in window]

            window_text = " ".join(window)
            if window_text in multi_word_fillers:
                found_fillers.append(window_text)

    return found_fillers


def infer_fillers(
    text: str,
    non_verbal_fillers: List[str],
    max_words: int = 2,
    language: str = "en",
) -> List[str]:
    """
    Per convention, all fillers should be marked with two commas (before and after).

    This function will extracs such fillers from the text.

    Example:
    ```
    "Look at this, I mean, isn't it great?" -> ["I mean"]
    ```

    Args:
        text (str): Text to extract fillers from
        max_words (int): Maximum number of words in a filler

    Returns:
        List[str]: List of fillers found in the text
    """
    fillers: List[str] = []

    if language == "zh":
        # Chinese punctuations use different characters
        delimiter = "。"
    else:
        delimiter = "."
    sentences = text.split(delimiter)
    sentences = [sentence.strip() for sentence in sentences]
    sentences = [sentence for sentence in sentences if sentence]
    sentences = [sentence.lower() for sentence in sentences]

    for sentence in sentences:
        delimiter = ","
        if language == "zh":
            # Chinese punctuations use different characters
            sentence = preprocess_zh_text(sentence)
        parts = sentence.split(delimiter)
        parts = [part.strip() for part in parts]

        for idx, part in enumerate(parts):
            # Ignore the first and last part
            if idx > 0 and idx < len(parts) - 1:
                # Check if the part is not too long
                if language == "zh":
                    # Chinese words / characters are not split by spaces
                    part_length = len(part)
                else:
                    part_length = len(part.split())
                if part_length <= max_words:
                    if part not in non_verbal_fillers:
                        fillers.append(part)

    return fillers


def match_stutters(
    text: str, non_verbal_fillers: List[str], language: str
) -> List[str]:
    """
    Counts the number of stutters in the text. Stutters are marked as a hyphenated word.

    Example:
        1. Um, yeah. Y-yeah. Yeah, I'd say so.
        2. I-I'm g-g-going

    Here the stutters are:
        1. Y-yeah
        2. I-I'm, g-g-going

    Args:
        text (str): Text to count stutters

    Returns:
        List[str]: List of stutters found in the text
    """
    delimiter = "-"
    stutters = []
    if language == "zh":
        # Chinese fillers are not separated by spaces
        pattern = re.compile(r"\s*--\s*")
        matches = pattern.finditer(text)

        for match in matches:
            start_idx = match.start()
            end_idx = match.end()
            is_partial = False

            if start_idx > 0:
                match_s_idx = start_idx - 1
            else:
                match_s_idx = start_idx
                is_partial = True
            if end_idx < len(text):
                match_e_idx = end_idx + 1
            else:
                match_e_idx = end_idx
                is_partial = True

            if not is_partial:
                if text[match_s_idx] == text[match_e_idx - 1]:
                    false_start = text[match_s_idx:match_e_idx]
                    stutters.append(false_start)
                else:
                    continue

        return stutters
    else:
        text_elements = text.split()
    text_elements = [element.lower() for element in text_elements]
    # Remove leading or trailing punctuation
    text_elements = [element.strip(".,!?-[]") for element in text_elements]

    for text_element in text_elements:
        if text_element in non_verbal_fillers:
            continue
        if delimiter in text_element:
            try:
                parts = text_element.split(delimiter)
                if parts[0][0] == parts[1][0]:
                    stutters.append(text_element)
            except IndexError:
                # Ignore if the split fails
                # 'like--' this will cause this
                pass

    return stutters


def match_word_repeats(text: str, language: str) -> List[str]:
    """
    Counts the number of word repeats in the text. Word repeats are marked
    as a word repeated multiple times.

    Example:
        1. Sally decided to go, go, go to the m-movies last, last night.

    Here the word repeats are:
        1. go, go, go
        2. last, last

    Args:
        text (str): Text to count word repeats

    Returns:
        List[str]: List of word repeats found in the text
    """

    word_repeats = []
    if language == "zh":
        # Chinese fillers are not separated by spaces
        # consider each character as a text element (word)

        # remove instances of '[crosstalk]'
        text = text.replace("[crosstalk]", "")

        text_elements = list(text)

        # remove punctuations
        characters_to_remove = constants.zh_punctuations_mapping.keys()

        text_elements = [
            element for element in text_elements if element not in characters_to_remove
        ]
    else:
        text_elements = text.split()
    text_elements = [element.lower() for element in text_elements]
    # Remove leading or trailing punctuation
    text_elements = [element.strip(".,!?-[]") for element in text_elements]

    idx = 0
    while idx < len(text_elements) - 1:
        if text_elements[idx] == text_elements[idx + 1]:
            # Check for longer repeats
            j = idx + 1
            while j < len(text_elements) and text_elements[j] == text_elements[idx]:
                j += 1
            if j - idx > 1:
                word_repeat_str = " ".join(text_elements[idx:j])
                if word_repeat_str != " ":
                    word_repeats.append(word_repeat_str)
            idx = j
        else:
            idx += 1

    return word_repeats


def match_false_starts(text: str, language: str) -> List[str]:
    """
    Counts the number of false starts in the text. False starts are context switches mid-word.

    Example:
        1. Did you go-- walk to the store?

    Here the false start is:
        1. go-- walk

    Args:
        text (str): Text to count false starts

    Returns:
        List[str]: List of false starts found in the text
    """

    false_starts = []

    if language == "zh":
        # Chinese fillers are not separated by spaces
        pattern = re.compile(r"\s*--\s*")
        matches = pattern.finditer(text)

        for match in matches:
            start_idx = match.start()
            end_idx = match.end()
            is_partial = False

            if start_idx > 0:
                match_s_idx = start_idx - 1
            else:
                match_s_idx = start_idx
                is_partial = True
            if end_idx < len(text):
                match_e_idx = end_idx + 1
            else:
                match_e_idx = end_idx
                is_partial = True

            if not is_partial:
                if text[match_s_idx] != text[match_e_idx - 1]:
                    false_start = text[match_s_idx:match_e_idx]
                    false_starts.append(false_start)
                else:
                    continue
            else:
                false_start = text[match_s_idx:match_e_idx]
                false_starts.append(false_start)

        return false_starts
    else:
        text_elements = text.split()
    text_elements = [element.lower() for element in text_elements]

    idx = 0
    while idx < len(text_elements) - 1:
        text_element = text_elements[idx]
        if text_element.endswith("--"):
            false_start = text_element + " " + text_elements[idx + 1]
            false_starts.append(false_start)

        idx += 1

    return false_starts


def get_features_df(
    transcript: Path, language: str, time_format: str = "%H:%M:%S.%f"
) -> pd.DataFrame:
    """
    Extract features from a TranscribeMe transcript.

    Args:
        transcript (Path): Path to the transcript file
        language (str): Language of the transcript

    Returns:
        pd.DataFrame: DataFrame with the extracted features
    """
    transcript_df = utils.parse_transcript_to_df(transcript, time_format=time_format)

    non_verbal_fillers = constants.non_verbal_fillers[language]
    non_verbal_fillers = [filler.lower() for filler in non_verbal_fillers]

    transcript_df = utils.add_basic_features(df=transcript_df, language=language)
    transcript_df["non_verbal_fillers"] = transcript_df["transcript"].apply(
        match_fillers, fillers=non_verbal_fillers, language=language
    )
    transcript_df["n_non_verbal_fillers"] = transcript_df["non_verbal_fillers"].apply(
        len
    )
    transcript_df["verbal_fillers"] = transcript_df["transcript"].apply(
        infer_fillers, non_verbal_fillers=non_verbal_fillers, language=language
    )
    transcript_df["n_verbal_fillers"] = transcript_df["verbal_fillers"].apply(len)
    transcript_df["stutters"] = transcript_df["transcript"].apply(
        match_stutters, non_verbal_fillers=non_verbal_fillers, language=language
    )
    transcript_df["n_stutters"] = transcript_df["stutters"].apply(len)
    transcript_df["word_repeats"] = transcript_df["transcript"].apply(
        match_word_repeats, language=language
    )
    transcript_df["n_word_repeats"] = transcript_df["word_repeats"].apply(len)
    transcript_df["false_starts"] = transcript_df["transcript"].apply(
        match_false_starts, language=language
    )
    transcript_df["n_false_starts"] = transcript_df["false_starts"].apply(len)
    transcript_df["n_disfluencies"] = (
        transcript_df["n_non_verbal_fillers"]
        + transcript_df["n_verbal_fillers"]
        + transcript_df["n_stutters"]
        + transcript_df["n_word_repeats"]
        + transcript_df["n_false_starts"]
    )

    return transcript_df
