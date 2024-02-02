"""
Represents OpenFace Quality Control metrics for a Openface Run
"""

from pathlib import Path

from pipeline import data
from pipeline.helpers import db
from pipeline.models.interview_roles import InterviewRole


class OpenFaceQcMetrics:
    """
    A class representing OpenFace quality control metrics for \
    a given interview name and role

    Attributes:
        interview_name : str
            The name of the interview.
        role : InterviewRole
            The role of primary person in the video.
        successful_frames_percentage : float
            The percentage of successful frames in the OpenFace quality control metrics.
        successful_frames_confidence_mean : float
            The mean confidence score of successful frames in the OpenFace quality control metrics.
    """

    def __init__(
        self,
        interview_name: str,
        role: InterviewRole,
        successful_frames_percentage: float,
        successful_frames_confidence_mean: float,
    ):
        self.interview_name = interview_name
        self.role = role
        self.successful_frames_percentage = successful_frames_percentage
        self.successful_frames_confidence_mean = successful_frames_confidence_mean

    def __str__(self):
        return f"""
        OpenFaceQcMetrics: [
            interview_name: {self.interview_name}
            role: {self.role}
            successful_frames_percentage: {self.successful_frames_percentage}
            successful_frames_confidence_mean: {self.successful_frames_confidence_mean}
        ]
        """

    def __repr__(self):
        return self.__str__()

    @staticmethod
    def get(
        config_file: Path,
        interview_name: str,
        role: InterviewRole,
    ) -> "OpenFaceQcMetrics":
        """
        Fetches OpenFace quality control metrics from the database.

        Args:
            config_file : Path
                The path to the configuration file.
            interview_name : str
                The name of the interview.
            role : InterviewRole
                The role of primary person in the video.

        Returns:
            OpenFaceQcMetrics
                An instance of OpenFaceQcMetrics.
        """

        of_path = data.get_openface_path(
            config_file=config_file, interview_name=interview_name, role=role
        )

        if of_path is None:
            raise ValueError(f"OpenFace path not found for {interview_name} and {role}")

        sql_query = f"""
        SELECT
            sucessful_frames_percentage, successful_frames_confidence_mean
        FROM openface_qc
        WHERE of_processed_path = '{of_path}'
        """

        results_df = db.execute_sql(config_file=config_file, query=sql_query)
        successful_frames_percentage = results_df["sucessful_frames_percentage"].values[
            0
        ]
        successful_frames_confidence_mean = results_df[
            "successful_frames_confidence_mean"
        ].values[0]

        return OpenFaceQcMetrics(
            interview_name=interview_name,
            role=role,
            successful_frames_percentage=successful_frames_percentage,
            successful_frames_confidence_mean=successful_frames_confidence_mean,
        )
