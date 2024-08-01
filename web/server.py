#!/usr/bin/env python
"""
Implements a simple file server that serves PDF and MP4 files.

Allows POST requests to put QC results in a database.
"""

import sys
from pathlib import Path

file = Path("/home/dm2637/dev/av-pipeline-v2/pipeline/runners/01_fetch_video.py")
parent = file.parent
root = None
for parent in file.parents:
    if parent.name == "av-pipeline-v2":
        root = parent
sys.path.append(str(root))

# remove current directory from path
try:
    sys.path.remove(str(parent))
except ValueError:
    pass

from datetime import datetime
from typing import Dict, Optional, List
import logging
import re

# from flask import Flask, send_file, request, Response
import flask
from flask_bootstrap import Bootstrap5
from flask_wtf import FlaskForm, CSRFProtect
import wtforms
import pandas as pd
from pydantic import BaseModel

from pipeline.helpers import db, utils
from pipeline import orchestrator

app = flask.Flask(__name__)
app.secret_key = "5u/9udJzoJWU2DqUSd7MgBpqNOb4ixA1VG2GRz/KI6gkvrm331pA4pTp"
bootstrap = Bootstrap5(app)
csrf = CSRFProtect(app)

logger = logging.getLogger(__name__)
logargs = {
    "level": logging.DEBUG,
    # "format": "%(asctime)s - %(process)d - %(name)s - %(levelname)s - %(message)s",
    "format": "%(message)s",
}
logging.basicConfig(**logargs)


# Transcript


class TranscriptElement(BaseModel):
    """
    Transcript element class.
    """

    turn: int
    speaker: str
    duration: int
    transcript: str
    timestamp: str
    redacted: bool = False


def parse_string_to_dict(input_string: str) -> Dict[str, float]:
    # Initialize an empty dictionary
    result_dict = {}

    # Use regular expression to extract the float value and the string key
    pattern = r"\((-?\d+\.\d+), '(\w+)'\)"
    matches = re.findall(pattern, input_string)

    # Convert the matches to dictionary entries
    for match in matches:
        value, key = match
        result_dict[key] = float(value)

    return result_dict


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
    - question_scores_mapping

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
                data[-1]["transcript"] += " " + line.strip()
                continue
        except ValueError:
            continue

        # try:
        #     score_map_str: str = text.split("[", 1)[1].split("]", 1)[0]
        #     score_map_dict: Optional[Dict[str, float]] = parse_string_to_dict(
        #         score_map_str
        #     )

        #     text = text.split("[", 1)[0]
        # except IndexError:
        #     score_map_dict = None

        speaker = speaker[:-1].strip()
        text = text.strip()
        element_data = {
            "turn": turn_idx,
            "speaker": speaker,
            "time": time,
            "transcript": text,
            # "question_scores_mapping": score_map_dict,
        }
        data.append(element_data)
        turn_idx += 1

    df = pd.DataFrame(data)

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


def transcript_df_to_transcript_elements(df: pd.DataFrame) -> List[TranscriptElement]:
    """
    Convert transcript DataFrame to a list of TranscriptElement objects.

    Args:
        df (pd.DataFrame): Transcript DataFrame.

    Returns
        List[TranscriptElement]: List of TranscriptElement objects.
    """

    elements = []
    for _, row in df.iterrows():
        time: str = row["time"]  # HH:MM:SS.SSS
        timestamp_mmss = f"{time.split(':')[1]}:{time.split(':')[2].split('.')[0]}"

        text = row["transcript"]

        # Check if text appears to be redacted
        # redacted text has {TEXT} in it
        re_pattern = r"\{.*\}"
        if re.search(re_pattern, text):
            redacted = True
        else:
            redacted = False

        element = TranscriptElement(
            turn=row["turn"],
            speaker=row["speaker"],
            duration=int(row["duration_ms"]),
            transcript=text,
            timestamp=timestamp_mmss,
            redacted=redacted,
        )
        elements.append(element)

    return elements


class Metadata:
    """
    Metadata class to store request metadata.
    """

    def __init__(self, request_obj: flask.Request):
        self.request = request_obj


@app.route("/payload=<path:file_path>")
def serve_file(file_path: str) -> flask.Response:
    """
    Serve PDF, MP4 and VTT files.

    Args:
        file_path (str): Path to the file to be served.

    Returns:
        Response: Flask response object.
    """
    file_path = file_path.lstrip("[").rstrip("]")  # Remove quotes from the file path
    file_p = Path(file_path)

    config_file = utils.get_config_file_path()
    data_root = orchestrator.get_data_root(config_file=config_file, enforce_real=True)
    fake_data_root = orchestrator.get_data_root(config_file=config_file)

    # Restrict access to the data root
    if not file_p.is_absolute():
        file_p = data_root / file_p
    else:
        # Check if the file path is within the data root
        if str(data_root) not in str(file_p) and str(fake_data_root) not in str(file_p):
            return flask.Response("Access denied.", status=403)

    if not file_p.exists():
        return flask.Response("File not found.", status=404)
    if file_path.endswith(".pdf"):
        return flask.send_file(file_path, mimetype="application/pdf")
    elif file_path.endswith(".mp4"):
        return flask.send_file(file_path, mimetype="video/mp4")
    elif file_path.endswith(".avi"):
        return flask.send_file(file_path, mimetype="video")
    elif file_path.endswith(".vtt"):
        return flask.send_file(file_path, mimetype="text/vtt")
    elif file_path.endswith(".png"):
        return flask.send_file(file_path, mimetype="image/png")
    else:
        return flask.Response("File type not supported.", status=400)


@app.route("/transcripts/view/<interview_name>")
def view_transcript(interview_name: str) -> flask.Response:
    """
    View Transcript.

    Args:
        interview_name (str): The name of the interview.

    Returns:
        Response: Flask response object.
    """
    config_file = utils.get_config_file_path()

    if interview_name == "random":
        query = """
        SELECT interview_name FROM transcript_files
        ORDER BY RANDOM()
        LIMIT 1;
        """
        interview_name_r = db.fetch_record(config_file=config_file, query=query)

        if not interview_name_r:
            return flask.Response("No interviews found", status=404)

        interview_name = interview_name_r

    query = f"""
    SELECT transcript_file FROM transcript_files
    WHERE interview_name = '{interview_name}';
    """

    transcript_file = db.fetch_record(config_file=config_file, query=query)

    if transcript_file:
        logger.debug(
            f"Interview {interview_name} has transcript file {transcript_file}"
        )
        transcript_file = Path(transcript_file)
        transcript_df = parse_transcript_to_df(transcript=transcript_file)
        transcript_elements = transcript_df_to_transcript_elements(transcript_df)

        query = f"""
        SELECT llm_interviewer_label FROM llm_speaker_identification
        WHERE llm_source_transcript = '{transcript_file}';
        """

        interviewer_label = db.fetch_record(config_file=config_file, query=query)
    else:
        logger.info(f"Interview {interview_name} has no transcript file")
        transcript_elements = None
        interviewer_label = None

    return flask.Response(
        flask.render_template(
            "transcript.html",
            metadata=Metadata(flask.request),
            title="Transcript",
            interview_name=interview_name,
            transcript_elements=transcript_elements,
            interviewer_label=interviewer_label,
        )
    )


@app.route("/transcripts/speaker_identification/<interview_name>")
def remove_speaker_identification(interview_name: str) -> flask.Response:
    """
    Remove speaker identification.

    Args:
        interview_name (str): The name of the interview.

    Returns:
        Response: Flask response object.
    """
    config_file = utils.get_config_file_path()

    query = f"""
    SELECT transcript_file FROM transcript_files
    WHERE interview_name = '{interview_name}';
    """

    transcript_file = db.fetch_record(config_file=config_file, query=query)

    if transcript_file is None:
        return flask.Response(
            f"No transcript found for interview {interview_name}", status=404
        )

    query = f"""
    DELETE FROM llm_speaker_identification
    WHERE llm_source_transcript = '{transcript_file}';
    """

    db.execute_queries(config_file=config_file, queries=[query])

    return flask.redirect(f"/transcripts/view/{interview_name}")  # type: ignore


@app.route("/interviews/view/<interview_name>")
def view_interview_frames(interview_name: str) -> flask.Response:
    """
    View interview frames.

    Args:
        interview_name (str): The name of the interview.

    Returns:
        Response: Flask response object.
    """
    config_file = utils.get_config_file_path()

    if interview_name == "random":
        query = """
        SELECT interview_name FROM video_quick_qc
        INNER JOIN decrypted_files ON video_quick_qc.video_path = decrypted_files.destination_path
        INNER JOIN interview_files ON decrypted_files.source_path = interview_files.interview_file
        INNER JOIN interviews USING (interview_path)
        ORDER BY RANDOM()
        LIMIT 1;
        """
        interview_name_r = db.fetch_record(config_file=config_file, query=query)

        if not interview_name_r:
            return flask.Response("No interviews found", status=404)

        interview_name = interview_name_r

    query = f"""
    SELECT video_path FROM video_quick_qc
    INNER JOIN decrypted_files ON video_quick_qc.video_path = decrypted_files.destination_path
    INNER JOIN interview_files ON decrypted_files.source_path = interview_files.interview_file
    INNER JOIN interviews USING (interview_path)
    INNER JOIN subjects USING(subject_id, study_id)
    WHERE interview_name = '{interview_name}';
    """

    videos_df = db.execute_sql(config_file=config_file, query=query)

    if videos_df.empty:
        video_paths = []
    else:
        video_paths = videos_df["video_path"].tolist()

    frames: List[str] = []

    for video_path in video_paths:
        v_path = Path(video_path)
        frames_path = v_path.parent / "frames" / v_path.stem
        v_frames = [str(f) for f in frames_path.glob("*.png")]
        v_frames = sorted(v_frames)
        frames.extend(v_frames)

    return flask.Response(
        flask.render_template(
            "interview_frames.html",
            metadata=Metadata(flask.request),
            title="Interview Frames",
            interview_name=interview_name,
            frames=frames,
            multiple_videos=len(video_paths) > 1,
        )
    )


@app.route("/interviews/openface/view/<interview_name>")
def view_openface_video(interview_name: str) -> flask.Response:
    """
    View interview, with OpenFace landmarks.

    Args:
        interview_name (str): The name of the interview.

    Returns:
        Response: Flask response object.
    """
    config_file = utils.get_config_file_path()

    if interview_name == "random":
        query = """
        SELECT interview_name FROM load_openface
        ORDER BY RANDOM()
        LIMIT 1;
        """
        interview_name_r = db.fetch_record(config_file=config_file, query=query)

        if not interview_name_r:
            return flask.Response("No interviews found", status=404)

        interview_name = interview_name_r

    query = f"""
    SELECT
        COALESCE(exported_assets.asset_destination, subject_of_processed_path) AS of_path
    FROM
        load_openface
    LEFT JOIN
        exported_assets
    ON
        exported_assets.asset_path = subject_of_processed_path
    WHERE
        load_openface.interview_name = '{interview_name}';
    """

    of_path = db.fetch_record(config_file=config_file, query=query)

    if not of_path:
        return flask.Response(
            f"No OpenFace data found for interview {interview_name}", status=404
        )

    of_path = Path(of_path)
    of_video = next(of_path.glob("openface_aligned.mp4"), None)

    if not of_video:
        return flask.Response(
            f"No OpenFace video found for interview at {of_path}", status=404
        )

    # Redirect to the video file
    return flask.redirect(f"/payload=[{of_video}]")  # type: ignore


@app.route("/")
def index() -> flask.Response:
    """
    Health (heartbeat) check.
    """
    host_name = flask.request.host
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    metadata: Metadata = Metadata(flask.request)
    print(metadata.request.path)

    return flask.Response(
        flask.render_template(
            "healthcheck.html",
            metadata=metadata,
            title="AV QC Portal",
            host_name=host_name,
            server_time=current_time,
        )
    )


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=45000, debug=True)
