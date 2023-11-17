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

# import subprocess
# import os
# from src.logging_utils import add_to_log

# def grab_uninspected_bams(dir):
#     """
#     Returns list of paths to uninspected BAM files (BAM files that have not
#     had samtools stats run on them yet) in a given directory

#     Args:
#         dir (str): directory to look in
#     """

#     dir_list = os.listdir(dir)
#     bam_list = []

#     for file in dir_list:
#         # Check if file is an unaligned bam
#         if file.endswith(".bam") and "aligned_sorted" in file:
#             # Check if file does not already have an aligned bam
#             # Will likely need to adjust this depending on file structure and 
#             # determined file naming convention
#             if f"{file.rsplit('.', 1)[0]}.qc" not in dir_list:
#                 bam_list.append(f"{dir}/{file}")
#     return bam_list


# def run_qc_single(input):    
#     """
#     Run samtools stats on a BAM file through Slurm.
#     """

#     samtools_stats_command = "samtools stats " + input
#     result = subprocess.run(samtools_stats_command.split(), shell = True, capture_output = True, text = True)


# def run_qc_all(input_dir):
#     """
#     Run samtools stats on all BAM files in the given directory.
#     """
    
#     add_to_log(f"Preparing to run QC on BAMs in {input_dir}.")
    
#     bam_list = grab_uninspected_bams(input_dir)
#     if len(bam_list) == 0:
#         add_to_log(f"Raising exception: No uninspected BAMs found in {input_dir}.")
#         raise Exception(f"No uninspected BAMs found in {input_dir}.")

#     for bam in bam_list:
#         run_qc_single(bam)
