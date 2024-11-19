#!/usr/bin/env python
"""
Identify speaker label from transcript using LLM model
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

import argparse
import logging
import random
from datetime import datetime
from typing import Any, Dict, List, Optional

import jinja2
import pandas as pd
from rich.logging import RichHandler

from pipeline import orchestrator
from pipeline.helpers import cli, db, llm, utils
from pipeline.helpers.timer import Timer
from pipeline.models.llm_speaker_identification import LlmSpeakerIdentification

MODULE_NAME = "llm_speaker_identification"

logger = logging.getLogger(MODULE_NAME)
logargs = {
    "level": logging.DEBUG,
    # "format": "%(asctime)s - %(process)d - %(name)s - %(levelname)s - %(message)s",
    "format": "%(message)s",
    "handlers": [RichHandler(rich_tracebacks=True)],
}
logging.basicConfig(**logargs)

console = utils.get_console()


noisy_modules: List[str] = ["httpcore.connection", "httpcore.http11"]
utils.silence_logs(noisy_modules=noisy_modules)


def get_file_to_process(config_file: Path, study_id: str) -> Optional[Path]:
    """
    Get file to process from the database.

    Args:
        config_file (Path): The path to the config file.
        study_id (str): The study id.

    Returns:
        Path: The path to the transcript file for speaker identification.
    """

    query = f"""
    WITH subjectIndentified AS (
        SELECT llm_source_transcript
        FROM llm_speaker_identification
        WHERE llm_role = 'subject'
    ), interviewerIdentified AS (
        SELECT llm_source_transcript
        FROM llm_speaker_identification
        WHERE llm_role = 'interviewer'
    )
    SELECT transcript_file
    FROM transcript_files
    LEFT JOIN interviews using (interview_name)
    WHERE interviews.study_id = '{study_id}' AND
        (
            transcript_file NOT IN (SELECT * FROM subjectIndentified) OR
            transcript_file NOT IN (SELECT * FROM interviewerIdentified)
        )
    ORDER BY RANDOM()
    LIMIT 1
    """

    transcript_file = db.fetch_record(
        config_file=config_file,
        query=query,
    )

    if transcript_file:
        return Path(transcript_file)

    return None


def parse_transcript_to_df(transcript: Path) -> pd.DataFrame:
    """
    Reads transcripts of the following format:

    ```text
    speaker: 00:00:00.000 transcript
    ```

    and converts it to a DataFrame with columns:
    - turn
    - speaker
    - time
    - transcript

    Note: Theid `disfluency` module relies on specific 'conventions' used
    by TranscribeMe's verbatim transcripts.

    Args:
        transcript (Path): Path to the transcript file

    Returns:
        pd.DataFrame: DataFrame with the transcript
    """

    with open(transcript, "r", encoding="utf-8") as f:
        lines = f.readlines()

    data = []
    turn_idx = 1
    for line in lines:
        try:
            speaker, time, text = line.split(" ", 2)
            try:
                pd.to_datetime(time, format="%H:%M:%S.%f")
            except ValueError:
                # add text to the previous line
                print(line)
                data[-1]["transcript"] += " " + line.strip()
                continue
        except ValueError:
            continue

        text = text.strip()
        element_data = {
            "turn": turn_idx,
            "speaker": speaker,
            "time": time,
            "transcript": text,
        }
        data.append(element_data)
        turn_idx += 1

    df = pd.DataFrame(data)

    if len(df) == 0:
        raise ValueError("No valid data found in the transcript file.")

    # Add a end_time column, by shifting the time column by one
    df["end_time"] = df["time"].shift(-1)

    # Compute the duration of each turn with millisecond-level precision
    df["duration_ms"] = pd.to_datetime(
        df["end_time"], format="%H:%M:%S.%f"
    ) - pd.to_datetime(df["time"], format="%H:%M:%S.%f")

    df["duration_ms"] = df["duration_ms"].dt.total_seconds() * 1000

    # Replace nan values with 0 on 'duration' column
    df["duration_ms"].fillna(0, inplace=True)

    # cast the turn, duration columns to int
    df["turn"] = df["turn"].astype(int)
    df["duration_ms"] = df["duration_ms"].astype(int)

    df.drop(columns=["end_time"], inplace=True)

    return df


def construct_prompt(
    transcript_path: Path,
    template: jinja2.Template,
) -> str:
    """
    Construct the prompt for the LLM model.

    Args:
        transcript_path (Path): Path to the transcript file.
        config_file (Path): Path to the config file.

    Returns:
        str: The prompt for the LLM model.
    """

    transcript_df = parse_transcript_to_df(transcript=transcript_path)

    TURNS_COUNT: int = 10

    if len(transcript_df) < TURNS_COUNT:
        temp_df = transcript_df
    else:
        # select TURNS_COUNT contiguous turns, starting from a random turn
        random_turn = random.randint(0, len(transcript_df) - TURNS_COUNT)
        temp_df = transcript_df.iloc[random_turn: random_turn + 10]

    transcript_text = ""
    for _, row in temp_df.iterrows():
        speaker = row["speaker"]
        transcript = row["transcript"]
        transcript_text += f"{speaker} {transcript}\n"

    prompt = template.render(transcript=transcript_text)

    return prompt


def process_transcript(
    transcript_path: Path, config_file: Path, n_tries: int = 3
) -> List[LlmSpeakerIdentification]:
    """
    Process the transcript and identify the speaker label.

    Args:
        transcript_path (Path): Path to the transcript file.
        config_file (Path): Path to the config file.

    Returns:
        List[LlmSpeakerIdentification]: The speaker identification model.
    """

    templates_root = orchestrator.get_templates_root(config_file=config_file)
    environment = jinja2.Environment(loader=jinja2.FileSystemLoader(templates_root))

    llm_config = utils.config(path=config_file, section="llm_speaker_identification")

    model = llm_config["ollama_model"]
    roles: List[str] = ["subject", "interviewer"]
    result_list: List[LlmSpeakerIdentification] = []

    with utils.get_progress_bar() as progress:
        role_identifaction_task = progress.add_task(
            "Identifying speaker role...", total=len(roles)
        )
        for role in roles:
            results: List[str] = []
            llm_metrics: List[Dict[str, Any]] = []
            progress.update(
                role_identifaction_task,
                description=f"Identifying speaker role: {role}",
            )
            prompt_template = llm_config[f"jinja2_prompt_template_{role}"]

            template = environment.get_template(prompt_template)

            speaker_id_task = progress.add_task(f"Identifying {role}...", total=n_tries)
            with Timer() as timer:
                for _ in range(n_tries):
                    try:
                        prompt = construct_prompt(
                            transcript_path=transcript_path,
                            template=template,
                        )
                    except ValueError as e:
                        logger.error(f"Error: {e}")
                        subject_result = LlmSpeakerIdentification(
                            llm_source_transcript=transcript_path,
                            ollama_model_identifier="default",
                            llm_role="subject",
                            llm_identified_speaker_label="undefined",
                            llm_confidence=0,
                            llm_metrics={},
                            llm_task_duration_s=0,
                            llm_timestamp=datetime.now(),
                        )
                        interviewer_result = LlmSpeakerIdentification(
                            llm_source_transcript=transcript_path,
                            ollama_model_identifier="default",
                            llm_role="interviewer",
                            llm_identified_speaker_label="undefined",
                            llm_confidence=0,
                            llm_metrics={},
                            llm_task_duration_s=0,
                            llm_timestamp=datetime.now(),
                        )

                        result_list.append(subject_result)
                        result_list.append(interviewer_result)

                        return result_list

                    prompt_task = progress.add_task(
                        "Prompting LLM model...", total=None
                    )
                    prompt_response = llm.prompt_llm(
                        prompt=prompt,
                        model=model,
                    )
                    progress.remove_task(prompt_task)

                    identified_speaker: str = prompt_response["message"]["content"]
                    identified_speaker = identified_speaker.strip()

                    llm_metrics.append(prompt_response)
                    results.append(identified_speaker)
                    progress.update(
                        speaker_id_task,
                        advance=1,
                        description=f"Identifing {role}... {results}",
                    )
                progress.update(role_identifaction_task, advance=1)

            # Construct Confidence metrics
            most_common_speaker = max(set(results), key=results.count)
            confidence = results.count(most_common_speaker) / len(results)

            llm_metrics_json: Dict[str, Any] = {"runs": llm_metrics}

            speaker_identification = LlmSpeakerIdentification(
                llm_source_transcript=transcript_path,
                ollama_model_identifier=model,
                llm_role=role,
                llm_identified_speaker_label=most_common_speaker,
                llm_confidence=confidence,
                llm_metrics=llm_metrics_json,
                llm_task_duration_s=timer.duration,
                llm_timestamp=datetime.now(),
            )
            result_list.append(speaker_identification)

    return result_list


def log_speaker_identification_result(
    config_file: Path, speaker_identification_list: List[LlmSpeakerIdentification]
) -> None:
    """
    Writes the speaker identification result to the database.

    Args:
        config_file (Path): Path to the config file.
        speaker_identification_list (List[LlmSpeakerIdentification]):
            The speaker identification model.

    Returns:
        None
    """
    sql_queries: List[str] = []
    for speaker_identification in speaker_identification_list:
        sql_query = speaker_identification.to_sql()
        sql_queries.append(sql_query)

    db.execute_queries(config_file=config_file, queries=sql_queries, show_commands=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog=MODULE_NAME,
        description="Module to split video file into individual streams.",
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
    studies = orchestrator.get_studies(config_file=config_file)
    data_root = orchestrator.get_data_root(config_file=config_file)

    COUNTER = 0

    logger.info(
        "[bold green]Starting speaker identification loop...", extra={"markup": True}
    )

    llm_config = utils.config(path=config_file, section="llm_speaker_identification")
    model = llm_config["ollama_model"]
    logger.info(f"Using model: {model}", extra={"markup": True})

    study_id = studies[0]
    logger.info(f"Starting with study: {study_id}", extra={"markup": True})

    while True:
        # Get file to process
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
                        message=f"Identified speaker for {COUNTER} files.",
                    )
                    COUNTER = 0

                # Snooze if no files to process
                orchestrator.snooze(config_file=config_file)
                study_id = studies[0]
                logger.info(
                    f"Restarting with study: {study_id}", extra={"markup": True}
                )
                continue
            else:
                study_id = studies[studies.index(study_id) + 1]
                logger.info(f"Switching to study: {study_id}", extra={"markup": True})
                continue

        COUNTER += 1
        logger.info(f"Processing file: {file_to_process}", extra={"markup": True})

        speaker_identification_list = process_transcript(
            transcript_path=file_to_process,
            config_file=config_file,
        )

        log_speaker_identification_result(
            config_file=config_file,
            speaker_identification_list=speaker_identification_list,
        )
