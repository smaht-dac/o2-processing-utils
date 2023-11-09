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
#   Script to run quality control tools (samtool stats) on aligned
#       PacBio HiFi BAMs and generate metrics.
#
########################################################################

import click
import subprocess

@click.command()
@click.help_option("--help", "-h")
@click.option(
    "-n",
    "--qm-name",
    required=True,
    type=str,
    help="Name of the Quality Metric",
)
@click.option(
    "-",
    "--input",
    required=True,
    type=str,
    help="Input PacBio BAM file to align",
)
def run_qc(input):
    
    """
    This script runs samtools stats on aligned PacBio HiFi reads and generates a list of metrics. The
    metrics are then parsed and arranged into a CSV file.
    """

    samtools_stats_command = "samtools stats " + input
    result = subprocess.run(samtools_stats_command.split(), shell = True, capture_output = True, text = True)


if __name__ == "__main__":
    run_qc()
