
from datetime import datetime, timezone
from src.config_utils import load_config, print_config, Config

def add_to_log(message: str):
    """
    Adds one line to the log file. UTC timestamp + the supplied message

    Args:
        message (str): text to add to the log
    """
    config : Config = load_config()
    with open(config.log_path, "a") as log_file:
        current_datetime = datetime.now(timezone.utc)
        current_date_time = current_datetime.strftime("%Y-%m-%d, %H:%M:%S %Z")
        log_file.write(f'{current_date_time}\t{message}\n')


def search_log(search_term: str):
    config : Config = load_config()
    with open(config.log_path, "r") as log_file:
        for line in log_file:
            if search_term.lower() in line.lower():
                print(line)


