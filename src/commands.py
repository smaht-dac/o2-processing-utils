
import click
from logging_utils import search_log
from env_utils import check_env_variable
from qc_utils import create_summary_qc_file
from constants import O2_PROCESSING_LOG_PATH
from run_pbmm2 import run_pbmm2_single, run_pbmm2_all
from run_qc import run_qc_single, run_qc_all

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


@click.command()
@click.help_option("--help", "-h")
@click.option(
    "-t",
    "--time",
    default="0-06:00:00",
    required=False,
    show_default=True,
    type=str,
    help="Amount of time to allocate to Slurm job (dd-HH:mm:ss)",
)
@click.option(
    "--mem",
    default="48G",
    required=False,
    show_default=True,
    type=str,
    help="Amount of memory to allocate to Slurm job",
)
@click.option(
    "-c",
    "--threads",
    required=False,
    default=32,
    show_default=True,
    type=int,
    help="Number of threads to allocate to Slurm job",
)
@click.option(
    "-u",
    "--mail-user",
    required=True,
    type=str,
    help="E-mail address to send Slurm reports to",
)
@click.option(
    "-",
    "--input",
    required=True,
    type=str,
    help="Input PacBio BAM file to align",
)
def run_pbmm2_single(time, mem, threads, mail_user, input_bam):
    run_pbmm2_single(time, mem, threads, mail_user, input_bam)

@click.command()
@click.help_option("--help", "-h")
@click.option(
    "-t",
    "--time",
    default="0-06:00:00",
    required=False,
    show_default=True,
    type=str,
    help="Amount of time to allocate to Slurm job (dd-HH:mm:ss)",
)
@click.option(
    "--mem",
    default="48G",
    required=False,
    show_default=True,
    type=str,
    help="Amount of memory to allocate to Slurm job",
)
@click.option(
    "-c",
    "--threads",
    required=False,
    default=32,
    show_default=True,
    type=int,
    help="Number of threads to allocate to Slurm job",
)
@click.option(
    "-u",
    "--mail-user",
    required=True,
    type=str,
    help="E-mail address to send Slurm reports to",
)
@click.option(
    "-",
    "--input_dir",
    required=True,
    type=str,
    help="Input directory containing unaligned BAM files to align",
)
def run_pbmm2_all(time, mem, threads, mail_user, input_dir):
    run_pbmm2_all(time, mem, threads, mail_user, input_dir)

@click.command()
@click.help_option("--help", "-h")
@click.option(
    "-",
    "--input",
    required=True,
    type=str,
    help="Input BAM file to run QC on.",
)
def run_qc_single(input):
    run_qc_single(input)


@click.command()
@click.help_option("--help", "-h")
@click.option(
    "-",
    "--input",
    required=True,
    type=str,
    help="Input directory containing BAM files to run QC on.",
)
def run_qc_all(input_dir):
    run_qc_all(input_dir)