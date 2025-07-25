import os
import pandas as pd
import logging

# === Logging Setup ===
logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s')

# === Paths ===
RAW_DATA_DIR = os.path.join(".", "data", "raw")
OUTPUT_DIR = os.path.join(".", "data", "transformed")
os.makedirs(OUTPUT_DIR, exist_ok=True)

# === Common Product & Sales Columns to Extract ===
PRODUCT_COLUMNS = ["product_id", "product_name", "category", "brand", "cost_price"]
SALES_COLUMNS = ["product_id", "order_date", "quantity", "units_sold", "sale_price", "sales", "marketplace"]

product_frames = []
sales_frames = []

# === Step 1: Scan and Analyze CSVs ===
logging.info("Scanning files in %s", RAW_DATA_DIR)
for file in os.listdir(RAW_DATA_DIR):
    if file.endswith(".csv"):
        path = os.path.join(RAW_DATA_DIR, file)
        try:
            df = pd.read_csv(path)

            logging.info(f"Reading {file} with shape {df.shape}")

            # Check if it's a product table
            prod_cols = [col for col in df.columns if col in PRODUCT_COLUMNS]
            sale_cols = [col for col in df.columns if col in SALES_COLUMNS]

            if len(prod_cols) >= 2:
                product_df = df[prod_cols].drop_duplicates()
                product_frames.append(product_df)
                logging.info(f"Added to product table with columns: {prod_cols}")

            if len(sale_cols) >= 2 and "order_date" in sale_cols:
                sales_df = df[sale_cols].dropna(subset=["order_date"])
                sales_frames.append(sales_df)
                logging.info(f"Added to sales table with columns: {sale_cols}")

        except Exception as e:
            logging.warning(f"Error reading {file}: {e}")

# === Step 2: Build Product Table ===
if product_frames:
    product_table = pd.concat(product_frames, ignore_index=True).drop_duplicates(subset=["product_id", "product_name"])
    product_path = os.path.join(OUTPUT_DIR, "product_table.csv")
    product_table.to_csv(product_path, index=False)
    logging.info(f"Product table saved with shape {product_table.shape}")
else:
    logging.warning("No product data found.")

# === Step 3: Build Sales Table ===
if sales_frames:
    sales_table = pd.concat(sales_frames, ignore_index=True)

    # Optional: Normalize column names
    if "quantity" in sales_table.columns and "units_sold" not in sales_table.columns:
        sales_table.rename(columns={"quantity": "units_sold"}, inplace=True)

    # Group by product and date if needed
    sales_table = sales_table.groupby(["product_id", "order_date"], as_index=False).sum(numeric_only=True)

    sales_path = os.path.join(OUTPUT_DIR, "sales_table.csv")
    sales_table.to_csv(sales_path, index=False)
    logging.info(f"Sales table saved with shape {sales_table.shape}")
else:
    logging.warning("No sales data found.")
