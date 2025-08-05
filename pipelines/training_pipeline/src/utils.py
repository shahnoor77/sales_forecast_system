import os
from dotenv import load_dotenv

def load_env():
    load_dotenv()
    return {
        "COMET_ML_API_KEY": os.getenv("COMET_ML_API_KEY"),
        "COMET_ML_PROJECT_NAME": os.getenv("COMET_ML_PROJECT_NAME"),
        "COMET_ML_WORKSPACE": os.getenv("COMET_ML_WORKSPACE"),
        "HOPSWORKS_API_KEY": os.getenv("HOPSWORKS_API_KEY"),
        "HOPSWORKS_PROJECT_NAME": os.getenv("HOPSWORKS_PROJECT_NAME"),
    }