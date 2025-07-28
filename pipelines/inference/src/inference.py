from fastapi import FastAPI
from pydantic import BaseModel
import joblib
import numpy as np
import uvicorn


# Load model and scaler

model = joblib.load("model.pkl")

# Optional: load the scaler if used in training
try:
    scaler = joblib.load("scaler.pkl")
except:
    scaler = None

# Define request schema

class PredictionRequest(BaseModel):
    sales_price: float
    quantity: int
    day_of_week: int  # 0 = Monday, ..., 6 = Sunday
    hour: int         # 0 to 23
    product_length: int  # Length of product name
    marketplace_code: int  # Encoded category of marketplace name

# Initialize FastAPI

app = FastAPI(title="Product Sales Predictor")

@app.get("/")
def read_root():
    return {"message": "Welcome to the Product Sales Prediction API"}

@app.post("/predict")
def predict(data: PredictionRequest):
    # Convert input to numpy array
    input_features = np.array([
        data.sales_price,
        data.quantity,
        data.day_of_week,
        data.hour,
        data.product_length,
        data.marketplace_code
    ]).reshape(1, -1)

    # Apply scaler if available
    if scaler:
        input_features = scaler.transform(input_features)

    # Predict
    prediction = model.predict(input_features)[0]
    return {
        "predicted_sub_total": round(prediction, 2)
    }


# Local run

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
