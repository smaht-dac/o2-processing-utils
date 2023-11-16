
O2_PROCESSING_LOG_PATH = "O2_PROCESSING_LOG_PATH"
SLURM_CONFIG_PATH = "SLURM_CONFIG_PATH"

REQUIRED_ENV_VARIABLES = {
    O2_PROCESSING_LOG_PATH: "Absolute path on O2 where the log file is or will be stored."
}

SAMTOOLS_STATS = "samtools stats"

SUPPORTED_QC_TOOLS = [
    SAMTOOLS_STATS
]

# This needs to be an environament variable!
REFERENCE_FILE_PATH = "/n/data1/hms/dbmi/park/SOFTWARE/REFERENCE/GRCh38_smaht/hg38_no_alt.fa"

DEFAULT_SLURM_CONFIG = {
    "allocated_time": "0-06:00:00",
    "allocated_memory": "48G",
    "allocated_threads": 32,
    "mail_user": ""
}
