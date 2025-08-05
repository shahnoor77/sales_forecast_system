import pandas as pd
import os
import hopsworks
from dotenv import load_dotenv

load_dotenv()

def store_data_to_feature_store():
    # Load CSV
    file_path = "data/transformed/final_standardized.csv"
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"{file_path} does not exist.")
    
    df = pd.read_csv(file_path)
    print(f"[INFO] Loaded data: {df.shape[0]} rows, {df.shape[1]} columns")

    # Load env vars
    project_name = os.getenv("HOPSWORKS_PROJECT_NAME")
    api_key = os.getenv("HOPSWORKS_API_KEY")

    if not project_name or not api_key:
        raise ValueError("Missing HOPSWORKS_PROJECT_NAME or HOPSWORKS_API_KEY in .env")

    # Login to Hopsworks
    project = hopsworks.login(project=project_name, api_key_value=api_key)
    feature_store = project.get_feature_store()
    print("[INFO] Connected to Hopsworks Feature Store")

    fg_name = "sales_record"
    fg_version = 1

    # Try to delete existing FG if it exists
    try:
        fg = feature_store.get_feature_group(name=fg_name, version=fg_version)
        print("[INFO] Existing feature group found. Deleting...")
        fg.delete()
    except:
<<<<<<< HEAD
        print("[INFO] No existing feature group found. Creating new one...")
=======
        print("No existing feature group, continuing")
>>>>>>> 91cdcdbdf0a9c629878c96b694b3c000345c56fb

    # Create a new feature group
    fg = feature_store.create_feature_group(
        name=fg_name,
        version=fg_version,
<<<<<<< HEAD
        primary_key=["product_name", "order_id"],
        description="Sales records for demand forecasting",
        online_enabled=False  # Keep offline to avoid Kafka
    )
=======
        primary_key=["product_name", "order_id"],  # adjust based on your data
        event_time ="created_at",  # adjust based on your data
        description="Sales records for demand forecasting",
        online_enabled=True,  # Set to True if you need online access
    )
    df = pd.read_csv("./data/transformed/final_standardized.csv")
>>>>>>> 91cdcdbdf0a9c629878c96b694b3c000345c56fb

    # Clean and prepare data
    df["product_name"] = df["product_name"].fillna("unknown_product").astype(str).str.slice(0, 100)
    df["created_at"] = pd.to_datetime(df["created_at"], errors='coerce')

    print("[INFO] Data cleaned. Starting insertion...")

<<<<<<< HEAD
    # Insert into Feature Store (offline only, logs shown)
    fg.insert(df, write_options={"start_offline": True, "wait_for_job": True})
    print("[SUCCESS] Data inserted into Hopsworks Feature Store (offline).")
=======

    # Insert data
    if fg is None:
        raise RuntimeError("Feature group is None. Aborting.")
    fg.insert(df, write_options={'start_offline': False, 'wait_for_job': True})
    print("Data inserted into feature store.")
>>>>>>> 91cdcdbdf0a9c629878c96b694b3c000345c56fb

if __name__ == "__main__":
    store_data_to_feature_store()
