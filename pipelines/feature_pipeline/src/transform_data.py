import os
import pandas as pd
import logging
from pathlib import Path
from datetime import datetime

# Setup logging (safe for Windows terminal)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger(__name__)

# Define input and output paths
BASE_DIR = Path(__file__).resolve().parents[1]
RAW_CSV_DIR = BASE_DIR / "data/raw"
OUTPUT_DIR = BASE_DIR / "data" / "transformed"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Define useful column keywords (can be extended)
USEFUL_COLUMNS = ['product', 'date', 'quantity', 'sale_price', 'sales', 'cost_price']

def find_useful_columns(df: pd.DataFrame) -> list:
    """Find useful columns based on keywords."""
    useful_cols = []
    for col in df.columns:
        for keyword in USEFUL_COLUMNS:
            if keyword.lower() in col.lower():
                useful_cols.append(col)
                break
    return useful_cols

def transform_csv_files():
    logger.info("Starting transformation pipeline...")

    all_data = []

    for file in RAW_CSV_DIR.glob("*.csv"):
        logger.info(f"Reading file: {file.name}")
        try:
            df = pd.read_csv(file)
            if df.empty:
                logger.warning(f"Skipping empty file: {file.name}")
                continue

            # Step 1: Identify and extract useful columns
            useful_cols = find_useful_columns(df)
            if not useful_cols:
                logger.warning(f"No useful columns in: {file.name}")
                continue

            logger.info(f"Selected columns: {useful_cols}")
            df = df[useful_cols].copy()

            # Step 2: Rename product identifier to 'product_id' (normalize)
            for col in df.columns:
                if 'product' in col.lower():
                    df.rename(columns={col: 'product_id'}, inplace=True)
                    break

            # Step 3: Normalize date column to 'order_date'
            for col in df.columns:
                if 'date' in col.lower():
                    df.rename(columns={col: 'order_date'}, inplace=True)
                    try:
                        df['order_date'] = pd.to_datetime(df['order_date'])
                    except Exception as e:
                        logger.warning(f"Failed to parse date in {file.name}: {e}")
                    break

            all_data.append(df)

        except Exception as e:
            logger.error(f"Error processing {file.name}: {e}")

    # Combine all data
    if not all_data:
        logger.warning("No valid data to process.")
        return

    full_df = pd.concat(all_data, ignore_index=True)
    logger.info(f"Combined shape: {full_df.shape}")
    #TODO: avoid droping too many rows.
    # Drop rows without product_id or order_date
    full_df = full_df.dropna(subset=['product_id', 'order_date'])

    # Step 4: Add 'month' column for grouping
    full_df['month'] = full_df['order_date'].dt.strftime('%B_%Y')

    # Step 5: Group and export
    for month, group in full_df.groupby('month'):
        out_path = OUTPUT_DIR / f"product_data_{month.lower()}.csv"
        group.drop(columns=['month'], inplace=True)
        group.to_csv(out_path, index=False)
        logger.info(f"Saved: {out_path.name} ({group.shape[0]} rows)")

    logger.info("Transformation pipeline complete.")

if __name__ == "__main__":
    transform_csv_files()
