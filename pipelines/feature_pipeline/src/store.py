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
    print(f"✅ Loaded data: {df.shape[0]} rows, {df.shape[1]} columns")

    # Load env vars
    project_name = os.getenv("HOPSWORKS_PROJECT_NAME")
    api_key = os.getenv("HOPSWORKS_API_KEY")

    if not project_name or not api_key:
        raise ValueError("Missing HOPSWORKS_PROJECT_NAME or HOPSWORKS_API_KEY in .env")

    # Login
    project = hopsworks.login(project=project_name, api_key_value=api_key)
    feature_store = project.get_feature_store()
    print("✅ Feature store initialized")

    # FG config
    fg_name = "sales_record"
    fg_version = 1

       # Always recreate feature group cleanly (for now)
    try:
        fg = feature_store.get_feature_group(name=fg_name, version=fg_version)
        print("⚠️ Deleting existing corrupted feature group")
        fg.delete()
    except:
        print("ℹ️ No existing feature group, continuing")

    # Recreate it
    fg = feature_store.create_feature_group(
        name=fg_name,
        version=fg_version,
        primary_key=["product_name", "order_id"],  # adjust based on your data
        #event_time ="created_at",  # adjust based on your data
        description="Sales records for demand forecasting",
        online_enabled=False,  # Set to True if you need online access
    )
    df = pd.read_csv("data/transformed/final_standardized.csv")

# Fill nulls in product_name (must be string for primary key)
    df["product_name"] = df["product_name"].fillna("unknown_product").astype(str).str.slice(0, 100)
    df["created_at"] = pd.to_datetime(df["created_at"], errors='coerce')  # Ensure datetime format

    df["product_name"] = df["product_name"].str.slice(0, 100)
    print("✅ Feature group created cleanly")


    # Insert data
    if fg is None:
        raise RuntimeError("❌ Feature group is None. Aborting.")
    fg.insert(df, write_options={'start_offline': True, 'wait_for_job': False})
    print("✅ Data inserted into feature store.")

if __name__ == "__main__":
    store_data_to_feature_store()
