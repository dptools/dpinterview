"""
Helper functions for sending notifications.
"""

from typing import Literal
from pathlib import Path

import apprise

from pipeline.helpers import utils


def notify(
    title: str,
    body: str,
    url: str,
    notify_type: Literal["info", "success", "warning", "failure"] = "info",
) -> None:
    """
    Sends a notification using the Apprise library.

    Args:
        title (str): The title of the notification.
        body (str): The body of the notification.
        url (str): The URL of the notification service.

    Returns:
        None
    """
    apobj = apprise.Apprise()

    apobj.add(url)

    apobj.notify(body=body, title=title, notify_type=notify_type)  # type: ignore

    return None


def send_notification(
    title: str,
    body: str,
    notify_type: Literal["info", "success", "warning", "failure"],
    config_file: Path,
) -> None:
    """
    Sends a notification using the Apprise library.

    Args:
        title (str): The title of the notification.
        body (str): The body of the notification.
        url (str): The URL of the notification service.

    Returns:
        None
    """

    config = utils.config(path=config_file, section="notifications")
    for url in config.values():
        if url.endswith(".key"):
            with open(url, "r", encoding="utf-8") as f:
                url = f.read().strip()
        notify(title=title, body=body, url=url, notify_type=notify_type)

    return None
