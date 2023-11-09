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
def run_pbmm2(time, mem, threads, mail_user, input):
    
    # Throw error if pbmm2=1.13.0 isn't installed -- needs to be installed in conda environment

    """
    This script gathers metrics from different tools and creates a QualityMetricGeneric item compatible JSON.

    Example usage:
    parse-qc \
        -n 'BAM Quality Metrics' \
        --metrics samtools /PATH/samtools.stats.txt \
        --metrics picard_CollectInsertSizeMetrics /PATH/picard_cis_metrics.txt \
        --additional-files /PATH/additional_output_1.pdf \
        --additional-files /PATH/additional_output_2.tsv \
        --output-zip metrics.zip
        --output-json qc_values.json

    This command will parse samtools and picard_CollectInsertSizeMetrics metrics from the files provided for each
    tool and create a qc.json file. It will also create a zip archive with all 4 provided files.
    """

    # prefix to the reference files; don't change for testing purposes.
    reference_fa="/n/data1/hms/dbmi/park/SOFTWARE/REFERENCE/GRCh38_smaht/hg38_no_alt.fa"
    preset="CCS"
    # Change name of output BAM
    output_bam=input + "_aligned_sorted"
    
    pbmm2_command = '"pbmm2 align --num-threads $threads --preset ' + preset + ' --strip --unmapped --log-level INFO --sort --sort-memory 1G --sort-threads 4 ' + reference_fa + ' ' + input + ' ' + output_bam + '"'
    sbatch_command = 'sbatch -J "pbmm2_step1b_align" -p park -A park_contrib -t 0-6:00:00 --mem=48G -c $threads --mail-type=ALL --mail-user=$email --wrap=' + pbmm2_command
    result = subprocess.run(sbatch_command.split(), shell = True, capture_output = True, text = True)

if __name__ == "__main__":
    run_pbmm2()
