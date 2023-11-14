
import os
from constants import O2_PROCESSING_LOG_PATH
from datetime import datetime, timezone


def add_to_log(message: str):
    """
    Adds one line to the log file. UTC timestamp + the supplied message

    Args:
        message (str): text to add to the log
    """

    with open(O2_PROCESSING_LOG_PATH, "a") as log_file:
        current_datetime = datetime.now(timezone.utc)
        current_date_time = current_datetime.strftime("%Y-%m-%d, %H:%M:%S %Z")
        log_file.write(f'{current_date_time}\t{message}\n')


def search_log(search_term: str):
    with open(O2_PROCESSING_LOG_PATH, "r") as log_file:
        for line in log_file:
            if search_term.lower() in line.lower():
                print(line)


