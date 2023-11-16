
import os
import json
from src.constants import DEFAULT_SLURM_CONFIG, SLURM_CONFIG_PATH


def load_slurm_config(verbose=False):

    if os.getenv(SLURM_CONFIG_PATH):
        with open(SLURM_CONFIG_PATH) as f:
            return json.load(f)
    else:
        if verbose:
            print(f"Default Slurm configuration loaded. You can set then envoronment variable {SLURM_CONFIG_PATH} to use your own configuration.")
        return DEFAULT_SLURM_CONFIG

def print_slurm_config():
    config = load_slurm_config()
    print(json.dumps(config, indent=2))



        
