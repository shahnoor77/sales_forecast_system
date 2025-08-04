import hopsworks
import pandas as pd
from hsfs.feature_view import FeatureView
from hsfs.feature_group import FeatureGroup
from dotenv import load_dotenv
load_dotenv()
import os
hopsworks_api_key = os.getenv("HOPSWORKS_API_KEY")
hopsworks_project_name = os.getenv("HOPSWORKS_PROJECT_NAME")


def init_hopsworks():
    project = hopsworks.login()  # Uses environment variables set by `load_dotenv()`
    fs = project.get_feature_store()
    return fs


def get_recent_data(fs, feature_group_name="sales-record", version=1, days=25):
    fg: FeatureGroup = fs.get_feature_group(feature_group_name, version)
    query_df = fg.select_all().read()

    query_df["created_at"] = pd.to_datetime(query_df["created_at"])
    recent_df = query_df.sort_values("created_at", ascending=False).groupby("product_name").head(days)
    return recent_df
