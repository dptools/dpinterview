"""
Represents metadata for a Video.
"""
from pathlib import Path

from pipeline import data
from pipeline.helpers import db
from pipeline.models.interview_roles import InterviewRole


class VideoMetadata:
    """
    A class representing metadata for a video.

    Attributes:
        interview_name : str
            The name of the interview.
        role : InterviewRole
            The role of primary person in the video.
        video_width : float
            The width of the video.
        video_height : float
            The height of the video.
    """

    def __init__(
        self,
        interview_name: str,
        role: InterviewRole,
        video_width: float,
        video_height: float,
    ):
        self.interview_name = interview_name
        self.role = role
        self.video_width = video_width
        self.video_height = video_height

    def __str__(self):
        return f"""
        VideoMetadata: [
            interview_name: {self.interview_name}
            role: {self.role}
            video_width: {self.video_width}
            video_height: {self.video_height}
        ]
        """

    def __repr__(self):
        return self.__str__()

    @staticmethod
    def get(
        config_file: Path,
        interview_name: str,
        role: InterviewRole,
    ) -> "VideoMetadata":
        """
        Fetches video metadata from the database.

        Args:
            config_file (Path): Path to the config file.
            interview_name (str): Name of the interview.
        """
        vs_path = data.get_interview_stream(
            config_file=config_file, interview_name=interview_name, role=role
        )

        if vs_path is None:
            raise FileNotFoundError(f"No video stream found for {interview_name}")

        sql_query = f"""
            SELECT fmv_width, fmv_height
            FROM ffprobe_metadata_video
            WHERE fmv_source_path = '{vs_path}'
        """

        resolution_df = db.execute_sql(config_file=config_file, query=sql_query)

        if resolution_df.empty:
            raise ValueError(f"No resolution found for {vs_path}")

        width = resolution_df.iloc[0]["fmv_width"]
        height = resolution_df.iloc[0]["fmv_height"]

        return VideoMetadata(
            interview_name=interview_name,
            role=role,
            video_width=width,
            video_height=height,
        )
