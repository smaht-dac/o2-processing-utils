import click, os
from src.logging_utils import search_log
from src.env_utils import check_env_variable, check_all_env_variables
from src.qc_utils import create_summary_qc_file
# from src.run_pbmm2 import run_pbmm2_single, run_pbmm2_all
# from src.run_qc import run_qc_single, run_qc_all
from src.config_utils import print_config
from src.Pbmm2Workflow import Pbmm2Workflow


@click.command()
@click.help_option("--help", "-h")
def cmd_print_config():
    """ Print out the config file"""
    print_config()


@click.command()
@click.help_option("--help", "-h")
@click.option(
    "-b",
    "--input-bam",
    required=False,
    type=str,
    help="Path to an unaligned BAM file to run the next step in the workflow",
)
@click.option(
    "-f",
    "--input-folder",
    required=False,
    type=str,
    help="Path to folder with unaligned BAM files to run the respective next steps in the workflow",
)
def cmd_run_pbmm2_workflow(input_bam, input_folder):
    """
    This script runs the full pbmm2 workflow on a given unaligned BAM file or all of the unaligned BAM files in
    a given folder. The script aligns the BAM files, runs some basic checks, runs samtools stats, and gathers
    various metrics on the files.
    """

    if not input_bam and not input_folder:
        print(
            "Error: Either a path to an unaligned BAM file or a path to a folder containing those must be provided"
        )
        return
    check_all_env_variables()

    if input_bam:
        working_dir = (
            "." if os.path.dirname(input_bam) == "" else os.path.dirname(input_bam)
        )
        pbmm2_workflow = Pbmm2Workflow(working_dir)
        file_name = os.path.basename(input_bam)
        pbmm2_workflow.resume_workflow_single(file_name)
    elif input_folder:
        pbmm2_workflow = Pbmm2Workflow(input_folder)
        pbmm2_workflow.resume_workflow_all()


@click.command()
@click.help_option("--help", "-h")
@click.option(
    "-b",
    "--input-bam",
    required=True,
    type=str,
    help="Path to an unaligned BAM file to run the next step in the workflow",
)
@click.option(
    "-s",
    "--workflow-step",
    required=True,
    type=str,
    help="Workflow step to reset. Valid options are 'qc', 'checks', and 'alignment'.",
)
def cmd_reset_pbmm2_workflow(input_bam, workflow_step):
    """
    This script resets a specific workflow step for a given input BAM file.
    """

    check_all_env_variables()
    working_dir = (
        "." if os.path.dirname(input_bam) == "" else os.path.dirname(input_bam)
    )
    pbmm2_workflow = Pbmm2Workflow(working_dir)
    file_name = os.path.basename(input_bam)
    pbmm2_workflow.reset(file_name=file_name, workflow_step=workflow_step)


@click.command()
@click.help_option("--help", "-h")
@click.option(
    "-s",
    "--search-term",
    required=True,
    type=str,
    help="Term to search for in the logs",
)
def cmd_search_log(search_term):
    """
    Searches the master log file for the specified terms and prints every hit to the terminal
    """
    check_all_env_variables()
    search_log(search_term)


@click.command()
@click.help_option("--help", "-h")
@click.option(
    "-q",
    "--qc-folder",
    required=True,
    type=str,
    help="Absolute path to folder where individual .qc files are located",
)
@click.option(
    "-s",
    "--summary-qc-path",
    required=True,
    type=str,
    help="Absolute path of the output summary QC file",
)
def cmd_create_summary_qc_file(qc_folder, summary_qc_path): 
    """ This scripts generates a summary QC file using the provided folder containing
        individual .qc files."""

    if not os.path.isabs(qc_folder):
        print("Error: Please provide the absolute path to the qc folder.")
        return
    if not os.path.isabs(summary_qc_path):
        print("Error: Please provide the absolute path to the summary QC file.")
        return
    create_summary_qc_file(qc_folder, summary_qc_path)


# @click.command()
# @click.help_option("--help", "-h")
# @click.option(
#     "-t",
#     "--time",
#     default="0-06:00:00",
#     required=False,
#     show_default=True,
#     type=str,
#     help="Amount of time to allocate to Slurm job (dd-HH:mm:ss)",
# )
# @click.option(
#     "--mem",
#     default="48G",
#     required=False,
#     show_default=True,
#     type=str,
#     help="Amount of memory to allocate to Slurm job",
# )
# @click.option(
#     "-c",
#     "--threads",
#     required=False,
#     default=32,
#     show_default=True,
#     type=int,
#     help="Number of threads to allocate to Slurm job",
# )
# @click.option(
#     "-u",
#     "--mail-user",
#     required=True,
#     type=str,
#     help="E-mail address to send Slurm reports to",
# )
# @click.option(
#     "-",
#     "--input",
#     required=True,
#     type=str,
#     help="Input PacBio BAM file to align",
# )
# def cmd_run_pbmm2_single(time, mem, threads, mail_user, input_bam):
#     run_pbmm2_single(time, mem, threads, mail_user, input_bam)

# @click.command()
# @click.help_option("--help", "-h")
# @click.option(
#     "-t",
#     "--time",
#     default="0-06:00:00",
#     required=False,
#     show_default=True,
#     type=str,
#     help="Amount of time to allocate to Slurm job (dd-HH:mm:ss)",
# )
# @click.option(
#     "--mem",
#     default="48G",
#     required=False,
#     show_default=True,
#     type=str,
#     help="Amount of memory to allocate to Slurm job",
# )
# @click.option(
#     "-c",
#     "--threads",
#     required=False,
#     default=32,
#     show_default=True,
#     type=int,
#     help="Number of threads to allocate to Slurm job",
# )
# @click.option(
#     "-u",
#     "--mail-user",
#     required=True,
#     type=str,
#     help="E-mail address to send Slurm reports to",
# )
# @click.option(
#     "-",
#     "--input_dir",
#     required=True,
#     type=str,
#     help="Input directory containing unaligned BAM files to align",
# )
# def cmd_run_pbmm2_all(time, mem, threads, mail_user, input_dir):
#     run_pbmm2_all(time, mem, threads, mail_user, input_dir)

# @click.command()
# @click.help_option("--help", "-h")
# @click.option(
#     "-",
#     "--input",
#     required=True,
#     type=str,
#     help="Input BAM file to run QC on.",
# )
# def run_qc_single(input):
#     run_qc_single(input)


# @click.command()
# @click.help_option("--help", "-h")
# @click.option(
#     "-",
#     "--input",
#     required=True,
#     type=str,
#     help="Input directory containing BAM files to run QC on.",
# )
# def cmd_run_qc_all(input_dir):
#     run_qc_all(input_dir)
