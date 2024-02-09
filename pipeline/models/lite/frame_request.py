"""
FrameRequest Class.

Used to represent a request for a frame of video.
"""

from pathlib import Path
from datetime import timedelta
from typing import Optional, List

from pipeline.helpers import db, dpdash
from pipeline.models.interview_roles import InterviewRole


class FrameRequest:
    """
    A class representing a request for a frame of video.

    Attributes:
        interview_name : str
            The name of the interview associated with the frame request.
        subject_id : str
            The ID of the subject associated with the frame request.
        study_id : str
            The ID of the study associated with the frame request.
        role : str
            The role of the user for whom the frame is requested.
        start_time : timedelta
            The start time of the requested frame.
        end_time : timedelta
            The end time of the requested frame.
    """

    def __init__(
        self,
        interview_name: str,
        subject_id: str,
        study_id: str,
        role: InterviewRole,
        start_time: timedelta,
        end_time: timedelta,
    ):
        self.interview_name = interview_name
        self.subject_id = subject_id
        self.study_id = study_id
        self.role = role
        self.start_time = start_time
        self.end_time = end_time

    def __str__(self):
        return f"""
        FrameRequest: [
            interview_name: {self.interview_name}
            subject_id: {self.subject_id}
            study_id: {self.study_id}
            role: {self.role}
            start_time: {self.start_time}
            end_time: {self.end_time}
        ]
        """

    @staticmethod
    def get_frame_number(request: "FrameRequest", config_file: Path) -> Optional[int]:
        """
        Gets a frame number from the database for a given FrameRequest. Gets the frame between the
        provided start and end times.

        Args:
            request (FrameRequest): An object containing the parameters for the database query.
            config_file_path (str): The path to the configuration file.
            SQL_TEMLATES_PATH (str): The path to the directory containing SQL templates.

        Returns:
            Optional[int]: The frame number, or None if the frame could not be retrieved.
        """
        sql_query = f"""
        WITH ranked_frames AS (
        SELECT frame,
            timestamp,
            RANK() OVER (
                ORDER BY timestamp
            ) AS rank,
            COUNT(*) OVER () AS total_count
        FROM openface_features
        where interview_name = '{request.interview_name}'
            and subject_id = '{request.subject_id}'
            and study_id = '{request.study_id}'
            and ir_role = '{request.role}'
            and timestamp BETWEEN '{request.start_time}' and '{request.end_time}'
        )
        SELECT frame,
            timestamp
        FROM ranked_frames
        WHERE MOD(
                rank - 1,
                cast(FLOOR(total_count / 2) as bigint)
            ) = 0
        ORDER BY timestamp;
        """

        frames = db.execute_sql(
            config_file=config_file, query=sql_query, db="openface_db"
        )

        if len(frames) < 2:
            return None
        else:
            return frames.iloc[1]["frame"]

    @staticmethod
    def get_frame_numbers(
        config_file: Path,
        interview_name: str,
        role: InterviewRole,
        start_time: timedelta,
        end_time: timedelta,
        frame_frequency: timedelta,
    ) -> List[Optional[int]]:
        """
        Gets a list of frame numbers for a given time range and frame frequency.

        Args:
            config_file (Path): The path to the configuration file.
            interview_name (str): The name of the interview associated with the frame request.
            role (str): The role of the user for whom the frame is requested.
            start_time (timedelta): The start time of the video.
            end_time (timedelta): The end time of the video.
            frame_frequency (timedelta):  For how much timedelta once a frame should be retrieved.

        Returns:
            List[Optional[int]]: A list of frame numbers, with None for any frames that
                could not be retrieved.
        """
        current_time = start_time
        frame_numbers: List[Optional[int]] = []
        increment_time = frame_frequency

        dpdash_dict = dpdash.parse_dpdash_name(interview_name)
        subject_id = dpdash_dict["subject"]
        study_id = dpdash_dict["study"]

        while current_time < end_time:
            frame_request = FrameRequest(
                interview_name=interview_name,
                subject_id=subject_id,  # type: ignore
                study_id=study_id,  # type: ignore
                role=role,
                start_time=current_time,
                end_time=current_time + increment_time,
            )
            frame_number = FrameRequest.get_frame_number(
                request=frame_request, config_file=config_file
            )
            frame_numbers.append(frame_number)

            current_time = current_time + increment_time

        return frame_numbers
