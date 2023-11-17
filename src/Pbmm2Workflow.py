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
import os
from pathlib import Path
from src.constants import SAMTOOLS_STATS
from src.logging_utils import add_to_log
from src.config_utils import load_config, print_config, Config
from src.file_utils import get_file_without_extension, remove_files
from src.qc_utils import parse_and_store_qc_outputs, QC_locations, QC_location

EXT_ALIGNMENT_RUNNING = "alignment_running"
EXT_CHECKS_COMPLETE = "checks_complete"
EXT_QC_RUNNING = "qc_running"
EXT_ALIGNED_SORTED = "aligned_sorted.bam"
EXT_SAMTOOLS_STATS = "stats.txt"
EXT_ALIGNMENT_SLURM_OUT = "out"


class Pbmm2Workflow:
    def __init__(self, working_directory):
        self.check_pbmm2_package() # TODO: Uncomment in prod
        self.config : Config = load_config()
        self.dir = working_directory

        print(f"Working directory: {self.dir}")
        print(f"Used configuration:")
        print_config()

    def resume_workflow_single(self, file_name):
        if self.is_workflow_complete(file_name):
            print(
                f"The workflow is complete for file {file_name}. Nothing else is done for this file."
            )
            return
        elif self.is_qc_complete(file_name):
            print(f"Parsing QCs and cleaning up for file {file_name}.")

            # TODO: Improve error handling
            file_name_without_ext = get_file_without_extension(file_name)
            path_to_stats = self.get_file_with_extension(file_name, EXT_SAMTOOLS_STATS)
            qc_location = QC_location(qc_tool=SAMTOOLS_STATS, output_path=path_to_stats)
            qc_locations = QC_locations([qc_location])
            qc_ouput_path = f"{self.dir}/qc/{file_name_without_ext}.qc"
            # Run QC parser which produces the .qc file
            add_to_log(f"Parsing QC outputs for {file_name} and storing .qc file")
            parse_and_store_qc_outputs(qc_locations, qc_ouput_path)

            self.cleanup(file_name)
            return
        elif self.is_qc_running(file_name):
            print(
                f"QC for {file_name} is currently running. Please rerun command when it is done."
            )
            return
        elif self.are_checks_complete(file_name):
            print(f"Running QC for file {file_name}")
            self.get_file_with_extension(file_name, EXT_ALIGNED_SORTED)
            path_to_aligned_bam = self.get_file_with_extension(
                file_name, EXT_ALIGNED_SORTED
            )
            self.run_qc(path_to_aligned_bam)
        elif self.is_alignment_complete(file_name):
            print(f"Running basic checks for file {file_name}")
            path_to_aligned_bam = self.get_file_with_extension(
                file_name, EXT_ALIGNED_SORTED
            )
            self.run_alignment_checks(path_to_aligned_bam)
        elif self.is_alignment_running(file_name):
            print(
                f"Alignment for {file_name} is currently running. Please rerun command when it is done."
            )
            return
        else:
            self.run_pbmm2(file_name)

    def resume_workflow_all(self):
        pathlist = Path(self.dir).rglob("*.bam")
        for path_obj in pathlist:
            # because path is an object not string
            path_to_file = str(path_obj)
            # Do not run the workflow on aligned BAM files
            if EXT_ALIGNED_SORTED in path_to_file:
                continue
            self.resume_workflow_single(path_to_file)

    def run_pbmm2(self, file_name):
        """
        Run pbmm2 on a single unaligned PacBio HiFi/Fiber-seq BAM through Slurm.
        """
        time = self.config.slurm_config.allocated_time
        mem = self.config.slurm_config.allocated_memory
        threads = self.config.slurm_config.allocated_threads
        mail_user = self.config.slurm_config.mail_user

        path_to_file = self.get_file_with_extension(file_name, "bam")

        log_stmt = f"Preparing to run pbmm2 on {path_to_file}. time={time}, mem={mem}, threads={threads}"
        add_to_log(log_stmt)
        print(log_stmt)

        PRESET = "CCS"
        output_bam = self.get_file_with_extension(file_name, EXT_ALIGNED_SORTED)
        slurm_out = f"{output_bam}.out"

        pbmm2_command = f'pbmm2 align --num-threads {threads} --preset {PRESET} --strip --unmapped --log-level INFO --sort --sort-memory 1G --sort-threads 4 {self.config.reference_sequence_path} {path_to_file} {output_bam}'
        sbatch_command = f'sbatch -J "pbmm2_align" -p park -A park_contrib -o {slurm_out} -t {time} --mem={mem} -c {threads} --mail-type=ALL --mail-user={mail_user} --wrap="{pbmm2_command}"'

        add_to_log(f"Submitting sbatch job to run pbmm2 on {path_to_file}.")
        try:
            result = subprocess.run(sbatch_command, shell = True, capture_output = True, text = True)
        except Exception as e:
            raise Exception(
                f"Error submitting sbatch job to run pbmm2 on {path_to_file}"
            )

        # Create the signal for the workflow that pbmm2 is running
        self.create_file_with_extension(file_name, EXT_ALIGNMENT_RUNNING)

    def run_alignment_checks(self, path_to_file):
        """
        Perform basic checks to confirm aligned BAM was properly generated
        """

        # TODO: Double check file paths are correct
        # TODO: Check if this is the correct way to handle failed checks (i.e. throwing errors)

        add_to_log(f"Running checks on {path_to_file}.")

        # Is this the correct place?
        slurm_out = f"{path_to_file}.{EXT_ALIGNMENT_SLURM_OUT}"

        # Check if Slurm files contain string "ERROR"; if yes, raise Excpetion
        ERROR_STRING = "ERROR"
        with open(slurm_out) as f:
            f_contents = f.read()
            if ERROR_STRING in f_contents or ERROR_STRING.lower() in f_contents:
                raise Exception(
                    f"Error submitting sbatch job to run pbmm2 on {path_to_file}"
                )

        # Perform header and EOF checks; raise Excpetion if failed
        try:
            subprocess.run(
                f"samtools quickcheck {path_to_file}",
                shell=True,
                text=True,
                capture_output=True,
                check=True,
            )
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
        mail_user = self.config.slurm_config.mail_user

        file_name = os.path.basename(path_to_file)
        output_stats = self.get_file_with_extension(file_name, EXT_SAMTOOLS_STATS)

        add_to_log(
            f"Preparing to run samtools stats on {path_to_file}. time={time}, mem={mem}, threads={threads}"
        )
        samtools_stats_command = f'samtools stats -@ {threads} {path_to_file} > {output_stats}'
        # TODO: Where does the Slurm output go?
        
        sbatch_command = f'sbatch -J "samtools_stats_qc" -p park -A park_contrib -t {time} --mem={mem} -c {threads} --mail-type=ALL --mail-user={mail_user} --wrap="{samtools_stats_command}"'


        add_to_log(f"Submitting samtools stats sbatch job on {path_to_file}.")
        try:
            result = subprocess.run(sbatch_command, shell = True, capture_output = True, text = True)
        except Exception as e:
            raise Exception(
                f"Error submitting samtools stats sbatch job on {path_to_file}."
            )

        # Create the signal for the workflow that QC is running
        self.create_file_with_extension(file_name, EXT_QC_RUNNING)

    def cleanup(self, file_name: str):
        add_to_log(f"Cleaning up temporary files for {file_name}.")
        # TODO: Add slurm output files
        files_to_remove = [
            self.get_file_with_extension(file_name, EXT_ALIGNMENT_RUNNING),
            self.get_file_with_extension(file_name, EXT_QC_RUNNING),
            self.get_file_with_extension(file_name, EXT_CHECKS_COMPLETE),
            self.get_file_with_extension(file_name, EXT_SAMTOOLS_STATS),
        ]
        remove_files(files_to_remove)

    def reset(self, file_name: str, workflow_step: str):
        path_to_file = self.get_file_with_extension(file_name, "bam")
        add_to_log(f"Resetting workflow step '{workflow_step}' for {path_to_file}.")
        if workflow_step == "qc":
            self.reset_qc(file_name)
        elif workflow_step == "checks":
            self.reset_checks(file_name)
        elif workflow_step == "alignment":
            self.reset_alignment(file_name)
        else:
            print("Error: workflow_step must be on of 'qc', 'checks', 'alignment'.")

    def reset_qc(self, file_name: str):
        files_to_remove = [
            self.get_file_with_extension(file_name, EXT_QC_RUNNING),
            f"{self.dir}/qc/{get_file_without_extension(file_name)}.qc",
            self.get_file_with_extension(file_name, EXT_SAMTOOLS_STATS),
        ]
        remove_files(files_to_remove)

    def reset_checks(self, file_name: str):
        self.reset_qc(file_name)
        files_to_remove = [
            self.get_file_with_extension(file_name, EXT_CHECKS_COMPLETE),
        ]
        remove_files(files_to_remove)

    def reset_alignment(self, file_name: str):
        self.reset_checks(file_name)
        files_to_remove = [
            self.get_file_with_extension(file_name, EXT_ALIGNMENT_RUNNING),
            self.get_file_with_extension(file_name, EXT_ALIGNED_SORTED),
        ]
        remove_files(files_to_remove)

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

    def does_file_with_extension_exist(self, file_name: str, extension: str):
        """Checks if the file with a given extension exists. E.g. if the original file is
            unaligned_file.bam, then this function checks for the existence of unaligned_file.{extension}

        Args:
            file_name (str): file name of the unaligned BAM
            extention (str): extension to check for
        """
        return os.path.isfile(self.get_file_with_extension(file_name, extension))

    def create_file_with_extension(self, file_name: str, extension: str):
        """Create an empty file for a given extension. E.g. if the original file is
            unaligned_file.bam, then this function creates unaligned_file.{extension}

        Args:
            file_name (str): file name of the unaligned BAM
            extention (str): extension the file is created with
        """
        open(self.get_file_with_extension(file_name, extension), "w").close()

    def get_file_with_extension(self, file_name, extension):
        file_name_without_ext = get_file_without_extension(file_name)
        return f"{self.dir}/{file_name_without_ext}.{extension}"

    def check_pbmm2_package(self):
        """
        Checks if correct pbmm2 version is installed.
        """

        # Throw error if pbmm2 v1.13.* isn't installed
        REQ_PACKAGE = "pbmm2 1.13"
        try:
            instl_package = subprocess.check_output(
                "pbmm2 --version", shell=True, text=True, stderr=subprocess.STDOUT
            )
            if REQ_PACKAGE not in instl_package:
                raise Exception(f"{REQ_PACKAGE} is a required package.")
        except subprocess.CalledProcessError as e:
            raise Exception(f"{REQ_PACKAGE} is a required package.") from e
