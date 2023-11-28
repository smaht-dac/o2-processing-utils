# o2-processing-utils
Tool for running and logging various bioinformatics tools on Harvard Medical School's high performance computing cluster O2.

## Installation
Run `pip install o2_processing_utils` to install the package. You also need samtools, pbmm2 1.13.*, and at least Python 3.8 installed, ideally in a conda environment.

## Usage
After installation, first ensure that the environment variable `O2_PROCESSING_CONFIG` is set to the absolute path of the .json file containing the path to the reference file, the path to the log file, and the resources to be allocated to the Slurm job running pbmm2. You can set the environment variable using the command

```
export O2_PROCESSING_CONFIG=/PATH_TO_CONFIG/config.json
```

An example configuration file (`config.example.json`) is provided.

To analyze a PacBio HiFi BAM, repeatedly run the following command from the command line:

```
o2p-run-pbmm2-workflow -b <input.bam>
```
The tool will automatically submit Slurm jobs to run alignment, peform basic checks, and gather QC metrics of the sample. Repeat runs of the command will automatically perform the next analysis if the previous step was completed successfully. To analyze multiple samples simulatenously, the user can pass the folder path containing the unaligned BAM files as an argument to the command.

The following commands are provided for additional functionality:

| Command                    | Description |
| -------------------------- | ----------- |
| o2p-print-config           | Print out O2_PROCESSING_CONFIG. |
| o2p-reset-pbmm2-workflow   | Reset a given workflow step for a given BAM file. This command only works for workflow runs that are incomplete. |
| o2p-create-summary-qc-file | Generate a summary QC file from a set of individual .qc files. |
| o2p-search-log             | Search the log for a given string. |

For additional information, you can type any of the following commands into the command line followed by the flag `--help`. If you forget any of the available commands, you can also type `o2p-` into the command line and then hit TAB twice. This will display all of the available functions.

The currently supported alignment tools are:
- pbmm2

The currently supported QC tools are:
- samtools (samtools stats)

## Development
To develop this package, clone this repo, make sure `poetry` is installed on your system and run `make install`.