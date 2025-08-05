import pandas as pd
import hopsworks
from src.config import HOPSWORKS_CONFIG

def store_to_feature_store(df: pd.DataFrame):
    project = hopsworks.login(project=HOPSWORKS_CONFIG['project'], api_key_value=HOPSWORKS_CONFIG['api_key'])
    fs = project.get_feature_store()

    fg_name = "sales_record"
    fg_version = 1

    try:
        fg = fs.get_feature_group(fg_name, fg_version)
        fg.delete()
    except:
        pass

    fg = fs.create_feature_group(
        name=fg_name,
        version=fg_version,
        primary_key=["product_name", "order_id"],
        event_time="created_at",
        description="Sales records for demand forecasting",
        online_enabled=True
    )

    df["product_name"] = df["product_name"].fillna("unknown").astype(str).str.slice(0, 100)
    df["created_at"] = pd.to_datetime(df["created_at"], errors='coerce')

    fg.insert(df, write_options={"start_offline": False, "wait_for_job": True})