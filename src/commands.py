
import click
from logging_utils import search_log
from env_utils import check_env_variable
from qc_utils import create_summary_qc_file
from constants import O2_PROCESSING_LOG_PATH

@click.command()
@click.help_option("--help", "-h")
@click.option(
    "-s",
    "--search-term",
    required=True,
    type=str,
    help="Term to search for in the logs",
)
def search_log(search_term):
    """Searches the master log file for the specified terms and prints every hit to the terminmal

    Args:
        search_term (str): Search term
    """
    check_env_variable(O2_PROCESSING_LOG_PATH)
    search_log(search_term)


@click.command()
@click.help_option("--help", "-h")
@click.option(
    "-q",
    "--qc-folder",
    required=True,
    type=str,
    help="Folder where individual .qc files are located",
)
@click.option(
    "-s",
    "--summary-qc-path",
    required=True,
    type=str,
    help="Path of the output summary QC file",
)
def create_summary_qc_file(qc_folder, summary_qc_path):
    create_summary_qc_file(qc_folder, summary_qc_path)

