# src/model_utils.py

import os
from comet_ml import API
import joblib
from dotenv import load_dotenv

load_dotenv()

COMET_ML_API_KEY = os.getenv('COMET_ML_API_KEY')
COMET_ML_WORKSPACE = os.getenv('COMET_ML_WORKSPACE')


''''def download_model(model_name: str, version: int):
    """Download model artifact from Comet model registry."""
    workspace = os.getenv("COMET_ML_WORKSPACE")  # Make sure this is set in your `.env` or shell
    registry_name = model_name  # typically same as model_name unless you used a custom name

    api = API(api_key=COMET_ML_API_KEY)

    # Fetch the model registry version
    model_registry = api.get_model(workspace=workspace, model_name=registry_name )
    #model_version = model_registry.get_version(version)

    # Download the model artifact
    download_path = "./model"  # You can customize this
    os.makedirs(download_path, exist_ok=True)

    model_file_path = model_registry.download(download_path)

    print(f" Model downloaded at: {model_file_path}")

    # Load and return the model object
    model = joblib.load(model_file_path)
    return model
'''
import os
import joblib

def load_model():
    # Relative path from this script to training's model folder
    model_path = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "../../training_pipeline/models/lightgbm_model.pkl")
    )

    if not os.path.exists(model_path):
        raise FileNotFoundError(f" Model file not found at: {model_path}")

    model = joblib.load(model_path)
    print(f" Model loaded from: {model_path}")
    return model
def load_model():
    # Relative path from this script to training's model folder
    model_path = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "../../training_pipeline/models/lightgbm_model.pkl")
    )

    if not os.path.exists(model_path):
        raise FileNotFoundError(f" Model file not found at: {model_path}")

    model = joblib.load(model_path)
    print(f" Model loaded from: {model_path}")
    return model

def load_scaler():
    # Relative path from this script to training's model folder
    scaler_path = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "../../training_pipeline/models/scaler.pkl")
    )

    if not os.path.exists(scaler_path):
        raise FileNotFoundError(f" Model file not found at: {scaler_path}")

    scaler = joblib.load(scaler_path)
    print(f" Model loaded from: {scaler_path}")
    return scaler
