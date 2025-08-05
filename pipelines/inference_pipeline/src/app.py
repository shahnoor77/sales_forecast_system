from fastapi import FastAPI
from dotenv import load_dotenv
from src.predictor import predict_sales
from src.hopsworks_utils import get_recent_data, init_hopsworks
from src.model_utils import load_model
from src.model_utils import load_scaler  
import pandas as pd
import uvicorn
import logging
import os

# Load environment variables
load_dotenv()

# Logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Constants
FEATURE_GROUP = "sales_record"
FEATURE_GROUP_VERSION = 1
DAYS_TO_FETCH = 25

# Initialize FastAPI app
app = FastAPI(title="Sales Forecast API", version="1.0")

# Load model and scaler
try:
    model = load_model()
    logger.info("Model loaded successfully.")
except Exception as e:
    logger.error(f" Failed to load model: {e}")
    model = None

# Connect to Hopsworks Feature Store
try:
    fs = init_hopsworks()
    logger.info("Connected to Hopsworks.")
except Exception as e:
    logger.error(f"Failed to initialize Hopsworks: {e}")
    fs = None


@app.get("/", tags=["Health"])
def health_check():
    """Health check endpoint."""
    return {"message": " Sales Forecast API is running."}


@app.get("/predict", tags=["Prediction"])
def predict_all():
    """
    Fetch recent data from Feature Store,
    run predictions, and return actual vs predicted sales
    grouped by product_name.
    """
    if model is None or fs is None:
        return {"error": "Model or Feature Store connection not initialized."}

    try:
        # Fetch recent 25-day data
        data = get_recent_data(
            fs=fs,
            feature_group_name=FEATURE_GROUP,
            version=FEATURE_GROUP_VERSION,
            days=DAYS_TO_FETCH
        )

        if data.empty:
            return {"error": "No data retrieved from Feature Store."}

        # Prepare actual sales for comparison
        actual = data[["product_name", "created_at", "sub_total"]].rename(columns={"sub_total": "actual_sales"})
        scaler = load_scaler()
        # Predict
        predictions = predict_sales(model=model, raw_df=data, scaler = scaler )

        # Merge predicted + actual
        combined = pd.merge(actual, predictions, on=["product_name", "created_at"], how="left")

        # Group by product_name
        result = {
            product: group.sort_values("created_at").to_dict(orient="records")
            for product, group in combined.groupby("product_name")
        }

        return result

    except Exception as e:
        logger.exception("Prediction failed.")
        return {"error": str(e)}


if __name__ == "__main__":
    uvicorn.run("src.app:app", host="0.0.0.0", port=8000, reload=True)
