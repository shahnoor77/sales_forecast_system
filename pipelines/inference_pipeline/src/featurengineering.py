
import pandas as pd

def engineer_features_for_inference(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["created_at"] = pd.to_datetime(df["created_at"])
    df["day_of_week"] = df["created_at"].dt.dayofweek
    df["hour"] = df["created_at"].dt.hour
    df["week_of_year"] = df["created_at"].dt.isocalendar().week
    df["product_length"] = df["product_name"].apply(lambda x: len(str(x)))
    df["marketplace_code"] = df["marketplace_name"].astype("category").cat.codes

    df.sort_values(["product_name", "created_at"], inplace=True)
    df["prev_day_sales"] = df.groupby("product_name")["sub_total"].shift(1).fillna(0)

    # Drop irrelevant
    drop_cols = ["order_id", "created_at", "product_name", "marketplace_name"]
    df.drop(columns=[col for col in drop_cols if col in df.columns], inplace=True)

    # Drop target if exists
    df.drop(columns=["sub_total"], errors="ignore", inplace=True)
    
    return df
