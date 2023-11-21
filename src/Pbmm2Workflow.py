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
EXT_ALIGNMENT_SLURM_OUT = "align_slurm_out"
EXT_ALIGNED_SORTED = "aligned_sorted.bam"
EXT_ALIGNED_SORTED_INDEXED = "aligned_sorted.bam.bai"
EXT_CHECKS_COMPLETE = "checks_complete"
EXT_QC_RUNNING = "qc_running"
EXT_QC_SLURM_OUT = "qc_slurm_out"
EXT_SAMTOOLS_STATS = "stats.txt"

REQ_PACKAGES = [
    ("pbmm2", "1.13"),
    ("samtools", ""),
    ]


class Pbmm2Workflow:
    def __init__(self, working_directory):
        self.check_packages(REQ_PACKAGES) # TODO: Uncomment in prod
        # self.check_pbmm2_package()
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
            qc_output_path = f"{self.dir}/qc/{file_name_without_ext}.qc"
            # Run QC parser which produces the .qc file
            add_to_log(f"Parsing QC outputs for {file_name} and storing .qc file")
            parse_and_store_qc_outputs(qc_locations, qc_output_path)

            self.cleanup(file_name)
            return
        elif self.is_qc_running(file_name):
            print(
                f"QC for file {file_name} is currently running. Please rerun command when it is done."
            )
            return
        elif self.are_checks_complete(file_name):
            print(f"Running QC for file {file_name}")
            self.run_qc(file_name)
        elif self.is_alignment_complete(file_name):
            print(f"Running basic checks for file {file_name}")
            self.run_alignment_checks(file_name)
        elif self.is_alignment_running(file_name):
            print(
                f"Alignment for file {file_name} is currently running. Please rerun command when it is done."
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

        log_stmt = f"Submitting sbatch job to run pbmm2 on {path_to_file}. time={time}, mem={mem}, threads={threads}"
        add_to_log(log_stmt)
        print(log_stmt)

        PRESET = "CCS"
        aligned_bam = self.get_file_with_extension(file_name, EXT_ALIGNED_SORTED)
        slurm_out = self.get_file_with_extension(file_name, EXT_ALIGNMENT_SLURM_OUT)

        pbmm2_command = f'pbmm2 align --num-threads {threads} --preset {PRESET} --strip --unmapped --log-level INFO --sort --sort-memory 1G --sort-threads 4 {self.config.reference_sequence_path} {path_to_file} {aligned_bam}'
        sbatch_command = f'sbatch -J "pbmm2_align" -p park -A park_contrib -o {slurm_out} -t {time} --mem={mem} -c {threads} --mail-type=ALL --mail-user={mail_user} --wrap="{pbmm2_command}"'

        try:
            result = subprocess.run(sbatch_command, shell = True, capture_output = True, text = True)
        except Exception as e:
            raise Exception(
                f"Error submitting sbatch job to run pbmm2 on file {path_to_file}"
            )

        # Create the signal for the workflow that pbmm2 is running
        self.create_file_with_extension(file_name, EXT_ALIGNMENT_RUNNING)

    def run_alignment_checks(self, file_name):
        """
        Perform basic checks to confirm aligned BAM was properly generated
        """

        add_to_log(f"Running checks on {file_name}.")
        path_to_aligned_bam = self.get_file_with_extension(
            file_name, EXT_ALIGNED_SORTED
        )

        # Perform header and EOF checks; raise Exception if failed
        try:
            subprocess.run(
                f"samtools quickcheck {path_to_aligned_bam}",
                shell=True,
                text=True,
                capture_output=True,
                check=True,
            )
        except subprocess.CalledProcessError as e:
            raise Exception(f"samtools quickcheck failed for file {path_to_aligned_bam}") from e

        self.create_file_with_extension(file_name, EXT_CHECKS_COMPLETE)

    def run_qc(self, file_name):
        # Hard-coding in sbatch allocated resources here
        # Should not need to allocate more resources than what's set
        time = "00-04:00:00"
        mem = "4G"
        threads = 2
        mail_user = self.config.slurm_config.mail_user

        aligned_bam = self.get_file_with_extension(file_name, EXT_ALIGNED_SORTED)
        slurm_out = self.get_file_with_extension(file_name, EXT_QC_SLURM_OUT)
        stats_txt = self.get_file_with_extension(file_name, EXT_SAMTOOLS_STATS)

        add_to_log(
            f"Submitting sbatch job to run samtools stats on {aligned_bam}. time={time}, mem={mem}, threads={threads}"
        )

        samtools_stats_command = f'samtools stats -@ {threads} {aligned_bam} > {stats_txt}'        
        sbatch_command = f'sbatch -J "samtools_stats_qc" -p park -A park_contrib -o {slurm_out} -t {time} --mem={mem} -c {threads} --mail-type=ALL --mail-user={mail_user} --wrap="{samtools_stats_command}"'

        try:
            result = subprocess.run(
                sbatch_command,
                shell = True,
                capture_output = True,
                text = True)
        except Exception as e:
            raise Exception(
                f"Error submitting samtools stats sbatch job for file {aligned_bam}."
            )

        # Create the signal for the workflow that QC is running
        self.create_file_with_extension(file_name, EXT_QC_RUNNING)

    def cleanup(self, file_name: str):
        add_to_log(f"Cleaning up temporary files for file {file_name}.")

        files_to_remove = [
            self.get_file_with_extension(file_name, EXT_ALIGNMENT_RUNNING),
            self.get_file_with_extension(file_name, EXT_ALIGNMENT_SLURM_OUT),
            self.get_file_with_extension(file_name, EXT_CHECKS_COMPLETE),
            self.get_file_with_extension(file_name, EXT_QC_RUNNING),
            self.get_file_with_extension(file_name, EXT_QC_SLURM_OUT),
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
            self.get_file_with_extension(file_name, EXT_QC_SLURM_OUT),
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
            self.get_file_with_extension(file_name, EXT_ALIGNED_SORTED_INDEXED),
            self.get_file_with_extension(file_name, EXT_ALIGNMENT_SLURM_OUT),
        ]
        remove_files(files_to_remove)

    def are_checks_complete(self, file_name: str):
        """Checks if checks have been run on the aligned BAMs

        Args:
            file_name (str): file name of the unaligned BAM
        """

        return self.does_file_with_extension_exist(file_name, EXT_CHECKS_COMPLETE)

    def is_alignment_running(self, file_name: str):
        """Checks if pbmm2 is currently running

        Args:
            file_name (str): file name of the unaligned BAM
        """

        return self.does_file_with_extension_exist(file_name, EXT_ALIGNMENT_RUNNING)

    def is_alignment_complete(self, file_name: str):
        """Checks if "*.aligned_sorted.bam exists" and if Slurm job was completed
        successfully.

        Args:
            file_name (str): file name of the unaligned BAM
        """

        if not self.does_file_with_extension_exist(file_name, EXT_ALIGNED_SORTED):
            return False

        # Check if Slurm files contain string "ERROR"; if yes, raise Excpetion
        slurm_out = self.get_file_with_extension(file_name, EXT_ALIGNMENT_SLURM_OUT)

        ERROR_STRING = "ERROR"
        with open(slurm_out) as f:
            f_contents = f.read()
            if ERROR_STRING in f_contents or ERROR_STRING.lower() in f_contents:
                raise Exception(
                    f"Error running pbmm2 sbatch job for file {file_name}"
                )
        
        return True

    def is_qc_running(self, file_name: str):
        """Checks if QC is currently running

        Args:
            file_name (str): file name of the unaligned BAM
        """

        return self.does_file_with_extension_exist(file_name, EXT_QC_RUNNING)

    def is_qc_complete(self, file_name: str):
        """Checks if QC is complete

        Args:
            file_name (str): file name of the unaligned BAM
        """

        samtools_stats = self.get_file_with_extension(file_name, EXT_SAMTOOLS_STATS)
        qc_slurm_out = self.get_file_with_extension(file_name, EXT_QC_SLURM_OUT)

        if self.does_file_with_extension_exist(file_name, EXT_SAMTOOLS_STATS):
            if os.path.getsize(samtools_stats) and not os.path.getsize(qc_slurm_out):
                return True
            else:
                return False
        else:
            return False

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
            extension (str): extension to check for
        """

        return os.path.isfile(self.get_file_with_extension(file_name, extension))

    def create_file_with_extension(self, file_name: str, extension: str):
        """Creates an empty file for a given extension. E.g. if the original file is
            unaligned_file.bam, then this function creates unaligned_file.{extension}

        Args:
            file_name (str): file name of the unaligned BAM
            extension (str): extension the file is created with
        """

        open(self.get_file_with_extension(file_name, extension), "w").close()

    def get_file_with_extension(self, file_name: str, extension: str):
        """Given the name of a file, returns the full file path with the specified
        extension. E.g. given "unaligned_file.bam" and extension "qc_slurm_out",
        returns "path/to/file/unaligned_file.qc_slurm_out"
        
        Args:
            file_name (str): file name of the uanligned BAM
            extension (str): extension the file is given
        """

        file_name_without_ext = get_file_without_extension(file_name)
        return f"{self.dir}/{file_name_without_ext}.{extension}"

    def check_packages(self, packages: list):
        for package in packages:
            self.check_package(package)

    def check_package(self, package):
        req_package = f"{package[0]} {package[1]}"
        try:
            instl_package = subprocess.check_output(
                f"{package[0]} --version", shell=True, text=True, stderr=subprocess.STDOUT
            )
            if "command not found" in instl_package:
                raise Exception(f"{req_package} is a required package.")
        except subprocess.CalledProcessError as e:
            raise Exception(f"{req_package} is a required package.") from e


    # def check_pbmm2_package(self):
    #     """
    #     Checks if correct pbmm2 version is installed.
    #     """

    #     # Throw error if pbmm2 v1.13.* isn't installed
    #     REQ_PACKAGE = "pbmm2 1.13"
    #     try:
    #         instl_package = subprocess.check_output(
    #             "pbmm2 --version", shell=True, text=True, stderr=subprocess.STDOUT
    #         )
    #         if REQ_PACKAGE not in instl_package:
    #             raise Exception(f"{REQ_PACKAGE} is a required package.")
    #     except subprocess.CalledProcessError as e:
    #         raise Exception(f"{REQ_PACKAGE} is a required package.") from e
