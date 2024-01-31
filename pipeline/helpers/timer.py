"""
Helper class for measuring the execution time of a block of code.
"""

from datetime import datetime
from typing import Optional


class Timer:
    """
    A context manager for measuring the execution time of a block of code.

    Usage:
    ```
    with Timer() as timer:
        # code to be timed
    print(f"Execution time: {timer.duration} seconds")
    ```
    """

    def __init__(self) -> None:
        self.start: Optional[datetime] = None
        self.end: Optional[datetime] = None
        self.duration: Optional[float] = None  # in seconds

    def __enter__(self) -> "Timer":
        self.start = datetime.now()
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback) -> None:
        self.end = datetime.now()
        if self.start is not None:
            self.duration = (self.end - self.start).total_seconds()
        else:
            raise ValueError("Timer was not started.")
