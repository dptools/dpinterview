"""
Helper functions for sending notifications.
"""

import logging
from pathlib import Path
from typing import List, Literal

import apprise

from pipeline.helpers import utils

logger = logging.getLogger(__name__)

# Silence logs from other modules
noisy_modules: List[str] = [
    "PIL.PngImagePlugin",
    "svglib.svglib",
    "matplotlib.font_manager",
]
for module in noisy_modules:
    logger.debug(f"Setting log level for {module} to INFO")
    logging.getLogger(module).setLevel(logging.INFO)


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
    try:
        config = utils.config(path=config_file, section="notifications")
    except ValueError:
        logger.debug("No notifications configured. Skipping.")
        return None
    for url in config.values():
        if url.endswith(".key"):
            with open(url, "r", encoding="utf-8") as f:
                url = f.read().strip()
        notify(title=title, body=body, url=url, notify_type=notify_type)

    return None
