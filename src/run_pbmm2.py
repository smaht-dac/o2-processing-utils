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

# import subprocess
# import re
# import os
# from src.constants import REFERENCE_FILE_PATH
# from src.logging_utils import add_to_log

# def check_pbmm2_package():
#     """
#     Checks if correct pbmm2 version is installed.
#     """

#     # Throw error if pbmm2 v1.13.* isn't installed
#     REQ_PACKAGE = "pbmm2 1.13"
#     try:
#         instl_package = subprocess.check_output("pbmm2 --version", shell=True, text = True, stderr = subprocess.STDOUT)
#         if REQ_PACKAGE not in instl_package:
#             raise Exception(f"{REQ_PACKAGE} is a required package.")
#     except subprocess.CalledProcessError as e:
#         raise Exception(f"{REQ_PACKAGE} is a required package.") from e        

# def grab_ubams(dir):
#     """
#     Returns list of paths to unaligned BAM files in a given directory

#     Args:
#         dir (str): directory to look in
#     """

#     dir_list = os.listdir(dir)
#     ubam_list = []

#     for file in dir_list:
#         # Check if file is an unaligned bam
#         if file.endswith(".bam") and "aligned_sorted" not in file:
#             # Check if file does not already have an aligned bam
#             # Will likely need to adjust this depending on file structure and 
#             # determined file naming convention
#             if f"{file.rsplit('.', 1)[0]}.aligned_sorted.bam" not in dir_list:
#                 ubam_list.append(f"{dir}/{file}")
#     return ubam_list


# def run_pbmm2_single(time, mem, threads, mail_user, input_bam):
#     """
#     Run pbmm2 on a single unaligned PacBio HiFi/Fiber-seq BAM through Slurm.
#     """
    
#     add_to_log(f"Preparing to run pbmm2 on {input_bam}. time={time}, mem={mem}, threads={threads}")
#     check_pbmm2_package()

#     PRESET = "CCS"
#     output_bam = f"{input_bam.rsplit('.', 1)[0]}.aligned_sorted.bam"
    
#     pbmm2_command = f'"pbmm2 align --num-threads {threads} --preset {PRESET} --strip --unmapped --log-level INFO --sort --sort-memory 1G --sort-threads 4 {REFERENCE_FILE_PATH} {input_bam} {output_bam}"'
#     sbatch_command = f'sbatch -J "pbmm2_step1b_align" -p park -A park_contrib -t {time} --mem={mem} -c {threads} --mail-type=ALL --mail-user={mail_user} --wrap=' + pbmm2_command

#     add_to_log(f"Submitting sbatch job for {input_bam}.")
#     try:
#         result = subprocess.run(sbatch_command.split(), shell = True, capture_output = True, text = True)
#     except Exception as e:
#         add_to_log(f"Raising exception: Error submitting sbatch job for {input_bam}.")
#         raise Exception(f"Error submitting sbatch job for {input_bam}")


# def run_pbmm2_all(time, mem, threads, mail_user, input_dir):
    
#     add_to_log(f"Preparing to run pbmm2 on unaligned BAMs in {input_dir}. time={time}, mem={mem}, threads={threads}")
#     check_pbmm2_package()
    
#     PRESET = "CCS"
#     ubam_list = grab_ubams(input_dir)
#     if len(ubam_list) == 0:
#         add_to_log(f"Raising exception: No unaligned BAMs found in {input_dir}.")
#         raise Exception(f"No unaligned BAMs found in {input_dir}.")

#     for ubam in ubam_list:
#         run_pbmm2_single(time, mem, threads, mail_user, ubam)

