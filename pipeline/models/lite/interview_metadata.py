"""
Represents metadata for an interview.
"""

from pathlib import Path
from typing import List
from datetime import timedelta

from pipeline import core
from pipeline.helpers import dpdash
from pipeline.models.interview_roles import InterviewRole


class InterviewMetadata:
    """
    A class representing metadata for an interview.

    Attributes:
        study (str): The name of the study.
        subject (str): The subject ID.
        visit (str): The visit number.
        total_visits (str): The total number of visits.
        study_day (str): The study day.
        time (str): The time of the interview.
        length (str): The length of the interview.
    """

    def __init__(
        self,
        study: str,
        subject: str,
        visit: str,
        interview_type: str,
        total_visits: str,
        study_day: str,
        time: str,
        length: str,
        has_interviewer_stream: bool,
    ):
        self.study = str(study)
        self.subject = str(subject)
        self.visit = str(visit)
        self.interview_type = interview_type
        self.total_visits = str(total_visits)
        self.study_day = str(study_day)
        self.time = str(time)
        self.length = str(length)
        self.has_interviewer_stream = has_interviewer_stream

    def get_params(self) -> List[str]:
        """
        Returns a list of all the interview metadata parameters.
        """
        return [
            self.study,
            self.subject,
            self.visit,
            self.total_visits,
            self.study_day,
            self.time,
            self.length,
        ]

    def get_params_col1_headers(self) -> List[str]:
        """
        Returns a list of headers for the first column of the interview metadata table.
        """
        return ["Study", "SubID", "Visit"]

    def get_params_col1(self) -> List[str]:
        """
        Returns a list of values for the first column of the interview metadata table.
        """
        return [self.study, self.subject, f"{self.visit} of {self.total_visits}"]

    def get_params_col2_headers(self) -> List[str]:
        """
        Returns a list of headers for the second column of the interview metadata table.
        """
        return ["StudyDay", "Time", "Length"]

    def get_params_col2(self) -> List[str]:
        """
        Returns a list of values for the second column of the interview metadata table.
        """
        return [self.study_day, self.time, self.length]

    def __str__(self):
        """
        Returns a string representation of the interview metadata object.
        """
        return f"""
        InterviewMetadata: [
            study: {self.study}
            subject: {self.subject}
            visit: {self.visit}
            total_visits: {self.total_visits}
            study_day: {self.study_day}
            time: {self.time}
            length: {self.length}
            has_interviewer_stream: {self.has_interviewer_stream}
        ]
        """

    def __repr__(self) -> str:
        """
        Returns a string representation of the interview metadata object.
        """
        return self.__str__()

    @staticmethod
    def get(config_file: Path, interview_name: str) -> "InterviewMetadata":
        """
        Returns the interview metadata for the given interview name.

        Args:
            config_file (Path): The path to the configuration file.
            interview_name (str): The name of the interview.

        Returns:
            InterviewMetadata: The interview metadata for the given interview name.
        """

        dpdash_dict = dpdash.parse_dpdash_name(interview_name)

        study = dpdash_dict["study"]
        subject = dpdash_dict["subject"]
        study_time = dpdash_dict["time_range"]  # day###
        study_time = study_time[3:]  # type: ignore Remove "day" prefix
        study_day = int(study_time)  # type: ignore

        visit_count = core.get_interview_visit_count(
            config_file=config_file, interview_name=interview_name
        )
        total_visits = core.get_total_visits_for_subject(
            config_file=config_file, subject_id=subject  # type: ignore
        )

        interview_type = core.get_interview_type(
            config_file=config_file, interview_name=interview_name
        )

        interview_duration = core.get_interview_duration(
            config_file=config_file, interview_name=interview_name
        )  # seconds
        interview_duration_td = timedelta(seconds=interview_duration)

        interview_datetime = core.get_interview_datetime(
            config_file=config_file, interview_name=interview_name
        )
        # split the datetime object into date and time
        # date = interview_datetime.date()
        time = interview_datetime.time()

        interviewer_of = core.get_openface_path(
            config_file=config_file,
            interview_name=interview_name,
            role=InterviewRole.INTERVIEWER,
        )
        if interviewer_of is not None:
            has_interviewer_stream = True
        else:
            has_interviewer_stream = False

        return InterviewMetadata(
            study=str(study),
            subject=str(subject),
            visit=str(visit_count),
            total_visits=str(total_visits),
            study_day=str(study_day),
            time=str(time),
            length=str(interview_duration_td),
            has_interviewer_stream=has_interviewer_stream,
            interview_type=interview_type,
        )
