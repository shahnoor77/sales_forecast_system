import os
import pandas as pd
import logging

# ---------------------- Logging Setup ----------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# ---------------------- File Paths -------------------------
INPUT_FILE = 'data/transformed/final_merged_data.csv'
OUTPUT_FILE = 'data/transformed/final_standardized.csv'

# ---------------------- Expected Columns -------------------
expected_columns = [
    "order_id", "created_at", "sub_total", "sales_price", "quantity",
    "product_id", "product_name", "brand", "category",
    "sub_category", "sub_sub_category", "seller_name", "marketplace_name"
]

# ---------------------- Rename Map -------------------------
rename_map = {
    "units_sold": "quantity",
    "unit_price": "sales_price",
    "amount": "sub_total",
    "naame": "seller_name",
    "name_mp": "marketplace_name"
}

# ---------------------- Transform Logic --------------------
def transform():
    logging.info("📥 Loading merged data CSV...")
    
    if not os.path.exists(INPUT_FILE):
        logging.error(f"Input file not found: {INPUT_FILE}")
        return

    df = pd.read_csv(INPUT_FILE)
    logging.info(f"✅ Data loaded: {df.shape[0]} rows, {df.shape[1]} columns")

    logging.info("🔁 Renaming columns...")
    df.rename(columns=rename_map, inplace=True)

    # Log current column names
    logging.info(f"📊 Columns after rename: {df.columns.tolist()}")

    # Filter columns safely
    available_columns = df.columns.tolist()
    filtered_columns = [col for col in expected_columns if col in available_columns]
    missing = list(set(expected_columns) - set(filtered_columns))

    if missing:
        logging.warning(f"⚠️ Skipping missing columns: {missing}")

    final_df = df[filtered_columns]
    logging.info(f"📐 Final data shape: {final_df.shape}")

    # Save output
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    final_df.to_csv(OUTPUT_FILE, index=False)
    logging.info(f"✅ Transformed data saved to: {OUTPUT_FILE}")
    
if __name__ == "__main__":
    transform()
