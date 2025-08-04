from src.fetch_data import fetch_all_tables
from src.data_cleaning import merge_all_csvs
from src.data_transformation import transform_data
from src.data_visualization import plot_sales_trend
from src.store_data import store_to_feature_store
from src.data_summary import summarize
RAW_DIR = "data/raw"
MERGED_PATH = "data/transformed/final_merged_data.csv"
TRANSFORMED_PATH = "data/transformed/final_standardized.csv"

def run_pipeline():
    fetch_all_tables()
    df_clean = merge_all_csvs(RAW_DIR, MERGED_PATH)
    summarize(df_clean, "Merged Data")

    df_transformed = transform_data(MERGED_PATH, TRANSFORMED_PATH)
    summarize(df_transformed, "Transformed Data")

    plot_sales_trend(df_transformed)
    store_to_feature_store(df_transformed)

if __name__ == "__main__":
    run_pipeline()
