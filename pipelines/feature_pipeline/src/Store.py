import os
import logging
import pandas as pd
import hopsworks
from hsfs.feature_group import FeatureGroup

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Constants
DATA_PATH = os.path.join("pipelines", "feature_pipeline", "data", "transformed", "final_standerdized.csv")
FEATURE_GROUP_NAME = "product_demand_features"
FEATURE_GROUP_VERSION = 1
PRIMARY_KEYS = ["product_id", "order_date"]
EVENT_TIME = "order_date"

def connect_to_hopsworks():
    try:
        project = hopsworks.login()
        fs = project.get_feature_store()
        logger.info("Connected to Hopsworks successfully.")
        return fs
    except Exception as e:
        logger.error(f"Failed to connect to Hopsworks: {e}")
        raise

def load_data(filepath):
    if not os.path.exists(filepath):
        logger.error(f"Data file not found at: {filepath}")
        raise FileNotFoundError(f"File not found: {filepath}")

    df = pd.read_csv(filepath)
    df[EVENT_TIME] = pd.to_datetime(df[EVENT_TIME])
    logger.info(f"Loaded data from {filepath} with shape {df.shape}")
    return df

def create_or_get_feature_group(fs, df):
    try:
        fg = fs.get_feature_group(
            name=FEATURE_GROUP_NAME,
            version=FEATURE_GROUP_VERSION
        )
        logger.info(f"Feature group '{FEATURE_GROUP_NAME}' v{FEATURE_GROUP_VERSION} already exists.")
    except:
        logger.info(f"Creating new feature group '{FEATURE_GROUP_NAME}' v{FEATURE_GROUP_VERSION}...")
        fg = fs.create_feature_group(
            name=FEATURE_GROUP_NAME,
            version=FEATURE_GROUP_VERSION,
            description="Daily product-level demand features",
            primary_key=PRIMARY_KEYS,
            event_time=EVENT_TIME,
            online_enabled=True
        )
    return fg

def insert_data_to_feature_store(fg, df):
    try:
        fg.insert(df, write_options={"wait_for_job": True})
        logger.info(f"Data inserted into feature group '{FEATURE_GROUP_NAME}' successfully.")
    except Exception as e:
        logger.error(f"Failed to insert data into feature store: {e}")
        raise

def main():
    logger.info("Starting Feature Store pipeline...")

    # Load data
    df = load_data(DATA_PATH)

    # Connect to Hopsworks
    fs = connect_to_hopsworks()

    # Create or get Feature Group
    fg = create_or_get_feature_group(fs, df)

    # Insert data into Feature Store
    insert_data_to_feature_store(fg, df)

    logger.info("Feature Store pipeline completed successfully.")

if __name__ == "__main__":
    main()
