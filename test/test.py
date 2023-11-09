########################################################################
#
#   Authors:
#       William Feng
#       Harvard Medical School
#       william_feng@gmail.com
#
#       Alexander Veit
#       Harvard Medical School
#       alexander_veit@hms.harvard.edu
#
#   Script to run pbmm2 on unaligned PacBio HiFi BAMs and generate
#       aligned BAM files.
#
########################################################################

import click
import subprocess

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
def run_pbmm2():
    
    pbmm2_command = '"pbmm2 align --num-threads $threads --preset $preset --strip --unmapped --log-level INFO --sort --sort-memory 1G --sort-threads 4 $reference_fa $input_bam $output_bam"'
    sbatch_command = 'sbatch -J "pbmm2_step1b_align" -p park -A park_contrib -t 0-6:00:00 --mem=48G -c $threads --mail-type=ALL --mail-user=$email --wrap=' + pbmm2_command
    print(sbatch_command)

if __name__ == "__main__":
    run_pbmm2()
