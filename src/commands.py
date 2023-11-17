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
    help="Workflow step to reset",
)
def cmd_reset_pbmm2_workflow(input_bam, workflow_step):
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
    """Searches the master log file for the specified terms and prints every hit to the terminmal

    Args:
        search_term (str): Search term
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
    help="Folder where individual .qc files are located",
)
@click.option(
    "-s",
    "--summary-qc-path",
    required=True,
    type=str,
    help="Path of the output summary QC file",
)
def cmd_create_summary_qc_file(qc_folder, summary_qc_path):
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
