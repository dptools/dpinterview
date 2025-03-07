"""
Module that provides helper functions for working with dpdash-compliant names.
"""

from typing import Dict, Union
from typing import Optional, List
from datetime import datetime


def get_days_between_dates(consent_date: datetime, event_date: datetime) -> int:
    """
    Returns the number of days between two dates.

    Args:
        consent_date (datetime): The consent date.
        event_date (datetime): The event date.

    Returns:
        int: The number of days between the two dates.
    """
    return abs((consent_date - event_date).days) + 1


def get_time_range(consent_date: datetime, event_date: datetime) -> str:
    """
    Generates a time range string in the format {consent_date}to{event_date}.

    Note: Consent day is considered as day1, event day is computed as the number of days
    between consent date and event date. If the event date is the same as the consent
    date, then event day is considered as day1: day1to1

    Args:
        consent_date (datetime): The consent date.
        event_date (datetime): The event date.

    Returns:
        str: The generated time range string.
    """
    # Get the time range
    if consent_date == event_date:
        time_range = "day1to0001"
    else:
        time_range = f"day1to{get_days_between_dates(consent_date, event_date):04d}"

    return time_range


def get_dpdash_timepoint(consent_date: datetime, event_date: datetime) -> str:
    """
    Generates a DPDash compliant timepoint string in the format dayXXX, where
    XXX is the number of days between consent date and event date.

    Note: Consent day is considered as day1, event day is computed as the number of
    days between consent date and event date. If the event date is the same as the
    consent date, then event day is considered as day1: day1

    Args:
        consent_date (datetime): The consent date.
        event_date (datetime): The event date.

    Returns:
        str: The generated timepoint string.
    """

    # Get the time range
    if consent_date == event_date:
        timepoint = "day0001"
    else:
        timepoint = f"day{get_days_between_dates(consent_date, event_date):04d}"

    return timepoint


def get_dpdash_name(
    study: str,
    subject: str,
    consent_date: Optional[datetime] = None,
    event_date: Optional[datetime] = None,
    time_range: Optional[str] = None,
    data_type: Optional[str] = None,
    category: Optional[str] = None,
    optional_tag: Optional[List[str]] = None,
) -> str:
    """
    Generates a DPDash Compliant name for a generated asset.
    study-subject-dataType_category{_optional_tag}-day{day_1}to{day_2}.csv

    ## Naming Convention
    | Field | Required | Example |
    | --- | --- | --- |
    | study | Yes | CAMI, BLS |
    | subject | Yes | ABC123, 9XY87Z |
    | consent_date | Yes | 2021-06-23 |
    | event_date | Yes | 2021-06-23 |
    | data_type | No | onsiteInterview |
    | category | No | video, audio |
    | optional_tag | No | ['test', 'test2'] |

    ## Delimiters
    | Field | Delimiter | Description |
    | --- | --- | --- |
    | study | _ | underscore |
    | subject | _ | underscore |
    | data_type | N/A | Use CamelCase |
    | category | N/A | Use CamelCase |
    | optional_tag | _ | underscore |

    ## Time Range
    Time Range is generated as {consent_date}to{event_date}: Example: NtoM, where N is the
    consent date and M is the event date.

    Note: Consent day is considered as day1, event day is computed as the number of days
    between consent date and event date. If the event date is the same as the consent date,
    then event day is considered as day1: day1to1

    ## Examples
    - CAMI-ABC123-onsiteInterview_video-day1to1.mp4: CAMI study, ABC123 subject,
    onsiteInterview data type, video category, consent date and event date are the same
    - CAMI-ABC123-onsiteInterview_day1to2.mp4: CAMI study, ABC123 subject,
    onsiteInterview data type, no category specified, consent date is say
    01/01/2021 and event date is 02/01/2021

    Args:
        study (str): The study ID.
        subject (str): The subject ID.
        consent_date (datetime): The consent date.
        event_date (datetime): The event date.
        time_range (str): The time range (instead of consent_date and event_date)
        data_type (Optional[str], optional): The data type. Defaults to None.
        category (Optional[str], optional): The category. Defaults to None.
        optional_tag (Optional[List[str]], optional): Optional tags. Defaults to None.

    Returns:
        str: The generated DPDash compliant name.
    """

    # Get the time range
    if not time_range:
        # Check if consent date and event date are provided
        if not consent_date or not event_date:
            raise ValueError(
                "Either 'time_range' or 'consent_date and event_date' must be provided"
            )
        time_range = get_dpdash_timepoint(consent_date, event_date)

    # Generate the name
    name = f"{study}-{subject}"

    if data_type:
        name += f"-{data_type}"

    if category:
        name += f"_{category}"

    # tags joined by underscore, e.g. test_test2
    # attached to the end of the name by
    # - e.g. CAMI-ABC123-onsiteInterview_video_test_test2-day1to1.mp4
    if optional_tag:
        name += f"_{('_').join(optional_tag)}"

    name += f"-{time_range}"

    return name


def parse_dpdash_name(name: str) -> Dict[str, Union[str, List[str], None]]:
    """
    Parses a string in the format of a dpdash file name and returns a dictionary
    with the parsed values.

    Args:
        name (str): The dpdash file name to parse.

    Returns:
        Dict[str, Union[str, List[str], None]]: A dictionary with the parsed values, including:
            - study (str): The study name.
            - subject (str): The subject ID.
            - data_type (str): The data type.
            - category (str or None): The category, if present.
            - optional_tags (List[str] or None): Any optional tags, if present.
            - time_range (str): The time range.
    """
    # Remove any extensions
    name = name.split(".")[0]

    parts = name.split("-")
    if len(parts) == 5:
        # enhanced with session number
        if 'session' not in name:
            raise ValueError(f"Invalid name: {name} - session number not found")
        parts = name.split("-", maxsplit=3)
    else:
        raise ValueError(f"Invalid name: {name} - wrong number of parts({len(parts)})")

    study, subject, data_type_category_tags, time_range = parts

    parts: List[str] = data_type_category_tags.split("_")
    data_type = parts[0]
    category = None
    optional_tag: Optional[List[str]] = None

    if len(parts) > 1:
        category = parts[1]

    if len(parts) > 2:
        optional_tag = parts[2:]

    return {
        "study": study,
        "subject": subject,
        "data_type": data_type,
        "category": category,
        "optional_tags": optional_tag,
        "time_range": time_range,
    }


def get_dpdash_name_from_dict(
    dpdash_dict: Dict[str, Union[str, List[str], None]]
) -> str:
    """
    Given a dictionary containing the required and optional fields for a dpdash name,
    returns the dpdash name as a string.

    Args:
        dpdash_dict: A dictionary containing the following keys:
            - study (str): The name of the study.
            - subject (str): The subject ID.
            - data_type (str): The type of data.
            - time_range (str): The time range of the data.
            - category (List[str], optional): A list of categories for the data.
                Defaults to None.
            - optional_tags (List[str], optional): A list of optional tags for the data.
                Defaults to None.

    Returns:
        The dpdash name as a string.

    Raises:
        ValueError: If any of the required fields are missing from the input dictionary.
    """
    required_fields = ["study", "subject", "data_type", "time_range"]
    optional_fields = ["category", "optional_tags"]

    for field in required_fields:
        if field not in dpdash_dict:
            raise ValueError(f"Missing required field: {field}")

    for field in optional_fields:
        if field not in dpdash_dict:
            dpdash_dict[field] = None

    name = get_dpdash_name(
        study=dpdash_dict["study"],  # type: ignore
        subject=dpdash_dict["subject"],  # type: ignore
        data_type=dpdash_dict["data_type"],  # type: ignore
        category=dpdash_dict["category"],  # type: ignore
        optional_tag=dpdash_dict["optional_tags"],  # type: ignore
        time_range=dpdash_dict["time_range"],  # type: ignore
    )
    return name
