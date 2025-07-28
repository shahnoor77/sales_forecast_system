import os
import logging
import hopsworks
import pandas as pd
from datetime import datetime
from sklearn.model_selection import train_test_split
import lightgbm as lgb
import comet_ml
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ModelTraining")

# Constants
FEATURE_VIEW_NAME = "product_demand_feature_view"
FEATURE_VIEW_VERSION = 1
FEATURE_GROUP_NAME = "product_demand_features"
FEATURE_GROUP_VERSION = 1
TARGET_COLUMN = "sales_quantity"

COMET_API_KEY = os.getenv("COMET_API_KEY")
COMET_PROJECT_NAME = os.getenv("COMET_PROJECT_NAME")
COMET_WORKSPACE = os.getenv("COMET_WORKSPACE")


def connect_to_hopsworks():
    try:
        project = hopsworks.login()
        fs = project.get_feature_store()
        return fs
    except Exception as e:
        logger.error(f"Error connecting to Hopsworks: {e}")
        raise


def get_or_create_feature_view(fs):
    try:
        feature_view = fs.get_feature_view(
            name=FEATURE_VIEW_NAME,
            version=FEATURE_VIEW_VERSION
        )
        logger.info("Feature view already exists. Skipping creation.")
    except:
        fg = fs.get_feature_group(
            name=FEATURE_GROUP_NAME,
            version=FEATURE_GROUP_VERSION
        )

        query = fg.select_all()
        feature_view = fs.create_feature_view(
            name=FEATURE_VIEW_NAME,
            version=FEATURE_VIEW_VERSION,
            description="Feature view for product demand forecasting",
            labels=[TARGET_COLUMN],
            query=query
        )
        logger.info("Created new feature view.")

    return feature_view


def perform_feature_engineering(df):
    df = df.copy()

    # Convert datetime if needed
    df['event_time'] = pd.to_datetime(df['event_time'])
    df['day_of_week'] = df['event_time'].dt.dayofweek
    df['day'] = df['event_time'].dt.day
    df['month'] = df['event_time'].dt.month
    df['weekofyear'] = df['event_time'].dt.isocalendar().week

    # Lag features
    df = df.sort_values(['product_id', 'event_time'])
    df['prev_day_sales'] = df.groupby('product_id')[TARGET_COLUMN].shift(1)
    df['rolling_7d_mean'] = df.groupby('product_id')[TARGET_COLUMN].transform(lambda x: x.shift(1).rolling(7).mean())
    df['rolling_7d_std'] = df.groupby('product_id')[TARGET_COLUMN].transform(lambda x: x.shift(1).rolling(7).std())

    df = df.dropna()
    return df


def train_model(df):
    try:
        X = df.drop(columns=[TARGET_COLUMN, 'event_time'])
        y = df[TARGET_COLUMN]

        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42
        )

        model = lgb.LGBMRegressor()
        model.fit(X_train, y_train)
        score = model.score(X_test, y_test)

        logger.info(f"Model trained with test R^2 score: {score:.4f}")
        return model, score, X_train, y_train, X_test, y_test

    except Exception as e:
        logger.error(f"Model training failed: {e}")
        raise


def log_model_to_comet(model, score):
    try:
        experiment = comet_ml.Experiment(
            api_key=COMET_API_KEY,
            project_name=COMET_PROJECT_NAME,
            workspace=COMET_WORKSPACE,
            auto_param_logging=True,
            auto_metric_logging=True
        )

        experiment.set_name("LightGBM Product Demand Model")
        experiment.log_metric("r2_score", score)
        experiment.log_model("lightgbm_model", model)
        logger.info("Model logged to Comet ML")
    except Exception as e:
        logger.error(f"Failed to log model to Comet ML: {e}")
        raise


def main():
    fs = connect_to_hopsworks()
    feature_view = get_or_create_feature_view(fs)

    try:
        df = feature_view.get_batch_data()
        logger.info("Fetched batch data from feature view")

        df = perform_feature_engineering(df)
        logger.info("Feature engineering complete")

        model, score, _, _, _, _ = train_model(df)
        log_model_to_comet(model, score)

    except Exception as e:
        logger.error(f"Pipeline execution failed: {e}")


if __name__ == "__main__":
    main()
