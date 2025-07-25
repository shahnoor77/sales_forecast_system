import os
import pandas as pd
import logging
from datetime import datetime

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s')
logger = logging.getLogger(__name__)

# Define paths
BASE_DIR = os.path.dirname(os.path.dirname(__file__))
RAW_DIR = os.path.join(".", "data", "raw")
OUTPUT_DIR = os.path.join(".", "data", "transformed")
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Common aliases for sales & product fields
DATE_COLS = ["date", "order_date", "created_on", "timestamp"]
PRODUCT_COLS = ["product_id", "product_code", "material_code", "pm_id"]
QUANTITY_COLS = ["quantity", "qty", "units"]

PRODUCT_TABLE = []
SALES_TABLE = []


def normalize_column(col):
    lower = col.lower()
    if any(alias in lower for alias in DATE_COLS):
        return "date"
    if any(alias in lower for alias in PRODUCT_COLS):
        return "product_id"
    if any(alias in lower for alias in QUANTITY_COLS):
        return "quantity"
    return col

def transform_files():
    PRODUCTS_TABLE = []
    SALES_TABLE = []

    for file in os.listdir(RAW_DIR):
        if file.endswith(".csv"):
            path = os.path.join(RAW_DIR, file)
            try:
                df = pd.read_csv(path)

                if df.empty:
                    logger.warning(f"Empty file skipped: {file}")
                    continue

                df.columns = [col.strip().lower().replace(" ", "_") for col in df.columns]

                # Normalize column names for product info
                product_columns = [col for col in df.columns if 'product' in col or 'sku' in col]
                sales_columns = [col for col in df.columns if 'sale' in col or 'quantity' in col or 'date' in col or 'order' in col]

                # Products
                if 'product_id' in df.columns or 'product_name' in df.columns:
                    product_df = df[[col for col in df.columns if col in ['product_id', 'product_name', 'category', 'price'] if col in df.columns]]
                    product_df = product_df.drop_duplicates().reset_index(drop=True)
                    PRODUCTS_TABLE.append(product_df)
                
                # Sales
                if 'product_id' in df.columns and 'quantity' in df.columns and any(c in df.columns for c in ['sale_date', 'order_date', 'date']):
                    date_col = [c for c in df.columns if c in ['sale_date', 'order_date', 'date']][0]
                    sales_df = df[['product_id', 'quantity', date_col]].copy()
                    sales_df.rename(columns={date_col: 'date'}, inplace=True)
                    sales_df = sales_df.drop_duplicates().reset_index(drop=True)
                    SALES_TABLE.append(sales_df)
                else:
                    logger.warning(f"Skipped {file} (no valid sales columns found)")

            except Exception as e:
                logger.error(f"Error processing {path}: {e}")

    if PRODUCTS_TABLE:
        all_products = pd.concat(PRODUCTS_TABLE).drop_duplicates().reset_index(drop=True)
        all_products.to_csv(os.path.join(OUTPUT_DIR, "products_table.csv"), index=False)
        logger.info(f"Saved products_table.csv with {len(all_products)} rows.")

    if SALES_TABLE:
        all_sales = pd.concat(SALES_TABLE).drop_duplicates().reset_index(drop=True)
        all_sales.to_csv(os.path.join(OUTPUT_DIR, "sales_table.csv"), index=False)
        logger.info(f"Saved sales_table.csv with {len(all_sales)} rows.")

if __name__ == "__main__":
    transform_files()
    logger.info("Data transformation completed.")