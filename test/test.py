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
#   Testing file.
#
########################################################################

import os
import subprocess


def run_pbmm2():

    REFERENCE_FILE_PATH = "/path/to/reference.fa"    
    threads = 64
    preset = "CCS"
    time = "0-06:00:00"
    mem = "48G"
    input_bam = "input.bam"
    output_bam = "output.bam"
    mail_user = "wfeng.slurm@gmail.com"


    pbmm2_command = f'"pbmm2 align --num-threads {threads} --preset {preset} --strip --unmapped --log-level INFO --sort --sort-memory 1G --sort-threads 4 {REFERENCE_FILE_PATH} {input_bam} {output_bam}"'
    sbatch_command = f'sbatch -J "pbmm2_step1b_align" -p park -A park_contrib -t {time} --mem={mem} -c {threads} --mail-type=ALL --mail-user={mail_user} --wrap=' + pbmm2_command
    print(sbatch_command)

def main():
    run_pbmm2()

if __name__ == "__main__":
    main()
