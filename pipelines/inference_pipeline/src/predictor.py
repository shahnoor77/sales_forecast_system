import pandas as pd
from sklearn.preprocessing import StandardScaler
from src.featurengineering import engineer_features_for_inference

def predict_sales(model, scaler: StandardScaler, raw_df: pd.DataFrame) -> pd.DataFrame:
    features_df = engineer_features_for_inference(raw_df)

    # Apply scaler from training
    X_scaled = scaler.transform(features_df)

    # Predict
    preds = model.predict(X_scaled)

    # Merge predictions back
    result = raw_df[["product_name", "created_at", "order_id"]].copy()
    result["predicted_sales"] = preds

    return result
