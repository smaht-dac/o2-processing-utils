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

import subprocess
import re
import os
from pathlib import Path
from src.constants import REFERENCE_FILE_PATH, SAMTOOLS_STATS
from src.logging_utils import add_to_log
from src.config_utils import load_slurm_config
from src.file_utils import get_file_without_extension
from src.qc_utils import parse_and_store_qc_outputs, QC_locations, QC_location

EXT_ALIGNMENT_RUNNING = "alignment_running"
EXT_ALIGNMENT_COMPLETE = "alignment_complete"
EXT_CHECKS_COMPLETE = "checks_complete"
EXT_QC_RUNNING = "qc_running"
EXT_ALIGNED_SORTED = "aligned_sorted.bam"
EXT_SAMTOOLS_STATS = "stats.txt"
EXT_ALIGNMENT_SLURM_OUT = "out"

####
## TODO: Add reset functionality
####


class Pbmm2Workflow:
    def __init__(self):
        # self.check_pbmm2_package() # TODO: Uncomment in prod
        self.slurm_config = load_slurm_config(verbose=True)
        self.dir = None

    def resume_workflow_single(self, path_to_file):
        # Should work with absolute and relative file paths
        if not self.dir:
            self.dir = (
                "."
                if os.path.dirname(path_to_file) == ""
                else os.path.dirname(path_to_file)
            )
        file_name = os.path.basename(path_to_file)

        if self.is_workflow_complete(file_name):
            print(f"The workflow is complete for file {file_name}. Nothing else is done for this file.")
            return
        elif self.is_qc_complete(file_name):
            print(f"Parsing QCs and cleaning up for file {file_name}.")
            # Run QC parser which produces the .qc file
            # TODO: Improve error handling
            file_name_without_ext = get_file_without_extension(file_name)
            path_to_stats = f"{self.dir}/{file_name_without_ext}.{EXT_SAMTOOLS_STATS}"
            qc_location = QC_location(qc_tool=SAMTOOLS_STATS, output_path=path_to_stats)
            qc_locations = QC_locations([qc_location])
            qc_ouput_path = f"{self.dir}/qc/{file_name_without_ext}.qc"
            parse_and_store_qc_outputs(qc_locations,qc_ouput_path)
            # TODO: Run cleanup function here.
            return
        elif self.is_qc_running(file_name):
            print(f"QC for {file_name} is currently running. Please rerun command when it is done.")
            return
        elif self.are_checks_complete(file_name):
            print(f"Running QC for file {file_name}")
            path_to_aligned_bam = f"{get_file_without_extension(path_to_file)}.{EXT_ALIGNED_SORTED}"
            self.run_qc(path_to_aligned_bam)
        elif self.is_alignment_complete(file_name):
            print(f"Running basic checks for file {file_name}")
            path_to_aligned_bam = f"{get_file_without_extension(path_to_file)}.{EXT_ALIGNED_SORTED}"
            self.run_qc(path_to_aligned_bam)
        elif self.is_alignment_running(file_name):
            print(f"Alignment for {file_name} is currently running. Please rerun command when it is done.")
            return
        else:
            self.run_pbmm2(path_to_file)

    def resume_workflow_all(self, path_to_folder):
        # TODO check if we need to take care of trailing slashes
        self.dir = path_to_folder

        pathlist = Path(path_to_folder).rglob("*.bam")
        for path_obj in pathlist:
            # because path is an object not string
            path_to_file = str(path_obj)
            # Do not run the workflow on aligned BAM files
            if EXT_ALIGNED_SORTED in path_to_file:
                continue
            self.resume_workflow_single(path_to_file)


    def run_pbmm2(self, path_to_file):
        """
        Run pbmm2 on a single unaligned PacBio HiFi/Fiber-seq BAM through Slurm.
        """
        time = self.slurm_config["allocated_time"]
        mem = self.slurm_config["allocated_memory"]
        threads = self.slurm_config["allocated_threads"]
        mail_user = self.slurm_config["mail_user"]
        
        add_to_log(f"Preparing to run pbmm2 on {path_to_file}. time={time}, mem={mem}, threads={threads}")

        PRESET = "CCS"
        file_name = os.path.basename(path_to_file)
        file_name_without_ext = get_file_without_extension(path_to_file)
        output_bam = f"{file_name_without_ext}.{EXT_ALIGNED_SORTED}"
        slurm_out = f"{output_bam}.out"


        # alignment_complete_file = f"{file_name_without_ext}.{EXT_ALIGNMENT_COMPLETE}"
        # TODO: Append to command: create ".alignment_complete" file. I think "... && touch {alignment_complete_file}"
        
        pbmm2_command = f'--wrap="pbmm2 align --num-threads {threads} --preset {PRESET} --strip --unmapped --log-level INFO --sort --sort-memory 1G --sort-threads 4 {REFERENCE_FILE_PATH} {path_to_file} {output_bam}"'
        sbatch_command = f'sbatch -J "pbmm2_align" -p park -A park_contrib -o {slurm_out} -t {time} --mem={mem} -c {threads} --mail-type=ALL --mail-user={mail_user} {pbmm2_command}'

        add_to_log(f"Submitting sbatch job to run pbmm2 on {path_to_file}.")
        try:
            result = subprocess.run(sbatch_command.split(), shell = True, capture_output = True, text = True)
        except Exception as e:
            #add_to_log(f"Raising exception: Error submitting sbatch job for {path_to_file}.")
            raise Exception(f"Error submitting sbatch job to run pbmm2 on {path_to_file}")
        
        # Create the signal for the workflow that pbmm2 is running
        self.create_file_with_extension(file_name, EXT_ALIGNMENT_RUNNING)


    def run_alignment_checks(self, path_to_file):
        """
        Perform basic checks to confirm aligned BAM was properly generated
        """

        # TODO: Double check file paths are correct
        # TODO: Check if this is the correct way to handle failed checks (i.e. throwing errors)

        add_to_log(f"Preparing to run checks on {path_to_file}.")

        slurm_out = f"{path_to_file}.{EXT_ALIGNMENT_SLURM_OUT}"

        # Check if Slurm files contain string "ERROR"; if yes, raise Excpetion
        ERROR_STRING = "ERROR"
        with open(slurm_out) as f:
            if ERROR_STRING in f.read() or ERROR_STRING.lower() in f.read():
                raise Exception(f"Error submitting sbatch job to run pbmm2 on {path_to_file}")

        # Perform header and EOF checks; raise Excpetion if failed
        try:
            subprocess.run(f"samtools quickcheck {path_to_file}",
                            shell = True, text = True,
                            capture_output = True, check = True)
        except subprocess.CalledProcessError as e:
            raise Exception(f"samtools quickcheck failed for {path_to_file}") from e

        file_name = os.path.basename(path_to_file)
        self.create_file_with_extension(file_name, EXT_CHECKS_COMPLETE)


    def run_qc(self, path_to_file):

        # Hard-coding in sbatch allocated resources here
        # Should not need to allocate more resources than what's set
        time = "00-04:00:00"
        mem = "4G"
        threads = 2
        mail_user = self.slurm_config["mail_user"]
        
        file_name_without_ext = get_file_without_extension(path_to_file)
        output_stats = f"{file_name_without_ext}.{EXT_SAMTOOLS_STATS}"
        
        add_to_log(f"Preparing to run samtools stats on {path_to_file}. time={time}, mem={mem}, threads={threads}")
        samtools_stats_command = f'--wrap="samtools stats -@ {threads} {path_to_file} > {output_stats}"'
        sbatch_command = f'sbatch -J "samtools_stats_qc" -p park -A park_contrib -t {time} --mem={mem} -c {threads} --mail-type=ALL --mail-user={mail_user}'

        add_to_log(f"Submitting samtools stats sbatch job on {path_to_file}.")
        try:
            result = subprocess.run(sbatch_command.split().append(samtools_stats_command),
                        shell = True,
                        capture_output = True,
                        text = True)
        except Exception as e:
            #add_to_log(f"Raising exception: Error submitting samtools stats sbatch job for {path_to_file}.")
            raise Exception(f"Error submitting samtools stats sbatch job on {path_to_file}.")

        # Create the signal for the workflow that QC is running
        file_name = os.path.basename(path_to_file)
        self.create_file_with_extension(file_name, EXT_QC_RUNNING)


    def are_checks_complete(self, file_name: str):
        """Checks the checks have been run on the aligned bams"

        Args:
            file_name (str): file name of the unaligned BAM
        """
        return self.does_file_with_extension_exist(file_name, EXT_CHECKS_COMPLETE)

    def is_alignment_running(self, file_name: str):
        """Checks if pbmm2 is currently running"

        Args:
            file_name (str): file name of the unaligned BAM
        """
        return self.does_file_with_extension_exist(file_name, EXT_ALIGNMENT_RUNNING)

    def is_alignment_complete(self, file_name: str):
        """Checks is the "*_aligned_sorted.bam exists"

        Args:
            file_name (str): file name of the unaligned BAM
        """
        if self.does_file_with_extension_exist(file_name, EXT_ALIGNED_SORTED):
            return True
        # TODO: We might have to check here for the slurm error file
        return False
    
    def is_qc_running(self, file_name: str):
        """Checks if QC is currently running"

        Args:
            file_name (str): file name of the unaligned BAM
        """
        return self.does_file_with_extension_exist(file_name, EXT_QC_RUNNING)
    
    def is_qc_complete(self, file_name: str):
        """Checks if QC is complete"

        Args:
            file_name (str): file name of the unaligned BAM
        """
        # TODO: Add verification logic
        return self.does_file_with_extension_exist(file_name, EXT_SAMTOOLS_STATS)

    def is_workflow_complete(self, file_name: str):
        """Checks for the existence of the ./qc/{file_name}.qc file

        Args:
            file_name (str): file name of the unaligned BAM
        """
        file_name_without_ext = get_file_without_extension(file_name)
        if os.path.isfile(f"{self.dir}/qc/{file_name_without_ext}.qc"):
            return True
        return False
    
    def does_file_with_extension_exist(self, file_name: str, extention: str):
        """Checks if the file with a given extension exists. E.g. if the original file is
            unaligned_file.bam, then this function checks for the existence of unaligned_file.{extension}

        Args:
            file_name (str): file name of the unaligned BAM
            extention (str): extension to check for
        """
        file_name_without_ext = get_file_without_extension(file_name)
        return os.path.isfile(f"{self.dir}/{file_name_without_ext}.{extention}")
    
    def create_file_with_extension(self, file_name: str, extention: str):
        """Create an empty file for a given extension. E.g. if the original file is
            unaligned_file.bam, then this function creates unaligned_file.{extension}

        Args:
            file_name (str): file name of the unaligned BAM
            extention (str): extension the file is created with
        """
        file_name_without_ext = get_file_without_extension(file_name)
        open(f"{self.dir}/{file_name_without_ext}.{extention}", 'w').close()

    def check_pbmm2_package(self):
        """
        Checks if correct pbmm2 version is installed.
        """

        # Throw error if pbmm2 v1.13.* isn't installed
        REQ_PACKAGE = "pbmm2 1.13"
        try:
            instl_package = subprocess.check_output("pbmm2 --version", shell=True, text = True, stderr = subprocess.STDOUT)
            if REQ_PACKAGE not in instl_package:
                raise Exception(f"{REQ_PACKAGE} is a required package.")
        except subprocess.CalledProcessError as e:
            raise Exception(f"{REQ_PACKAGE} is a required package.") from e        