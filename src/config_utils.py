
import os
import json
from src.constants import O2_PROCESSING_CONFIG
from pydantic import (BaseModel, RootModel, field_validator, ValidationInfo,)
from rich import print

# TODO: Add validators for these models
class SlurmConfig(BaseModel):
    allocated_time: str
    allocated_memory: str
    allocated_threads: int
    mail_user: str

    @field_validator('allocated_time', 'allocated_memory', 'mail_user')
    @classmethod
    def check_for_spaces(cls, v: str, info: ValidationInfo) -> str:
        if ' ' in v:
            # info.field_name is the name of the field being validated
            raise ValueError(f'{info.field_name} cannot contain a space.')
        return v


class Config(BaseModel):
    reference_sequence_path: str
    log_path: str
    slurm_config: SlurmConfig


def load_config():

    if os.getenv(O2_PROCESSING_CONFIG):
        with open(os.getenv(O2_PROCESSING_CONFIG)) as f:
            c = json.load(f)
        return Config(**c)
    else:
        raise Exception(f"Configuration file not found. Please set the environment variable {O2_PROCESSING_CONFIG} with the path to your config file.")

def print_config():
    config : Config = load_config()
    print(config)



        
