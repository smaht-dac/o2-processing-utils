
import os
from src.constants import REQUIRED_ENV_VARIABLES


def check_all_env_variables():

    for v in REQUIRED_ENV_VARIABLES.keys():
        if not os.getenv(v):
            raise Exception(f"{v} is a required environment variable: {REQUIRED_ENV_VARIABLES[v]}")
        
def check_env_variable(v):

    if v not in REQUIRED_ENV_VARIABLES:
        raise Exception(f"Environment variable {v} is not required")

    if not os.getenv(v):
        raise Exception(f"{v} is a required environment variable: {REQUIRED_ENV_VARIABLES[v]}")
        
