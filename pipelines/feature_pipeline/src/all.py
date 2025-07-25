import os
import pandas as pd
import logging
from sqlalchemy import create_engine, inspect
from dotenv import load_dotenv
from urllib.parse import quote_plus

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s')
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

# Create SQLAlchemy engine
encoded_password = quote_plus(DB_PASSWORD)
DATABASE_URL = f"mysql+pymysql://{DB_USER}:{encoded_password}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DATABASE_URL)

# Output directory
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "raw")
os.makedirs(OUTPUT_DIR, exist_ok=True)

# List to track newly downloaded tables
newly_downloaded = []

def fetch_all_non_empty_tables():
    """Fetch all non-empty tables from the database and store as CSV, skipping already downloaded ones."""
    inspector = inspect(engine)
    tables = inspector.get_table_names()

    for table_name in tables:
        output_file = os.path.join(OUTPUT_DIR, f"{table_name}.csv")

        # Skip if file already exists
        if os.path.exists(output_file):
            logger.info(f"Skipping already downloaded table: {table_name}")
            continue

        try:
            logger.info(f"Processing table: {table_name}")
            df = pd.read_sql_query(f"SELECT * FROM `{table_name}`", engine)

            if df.empty:
                logger.warning(f"Skipping empty table: {table_name}")
                continue

            df.to_csv(output_file, index=False)
            newly_downloaded.append(output_file)
            logger.info(f"Saved {len(df)} rows from table '{table_name}' to '{output_file}'")

        except Exception as e:
            logger.error(f"Error processing table {table_name}: {e}")

    # Print summary of newly downloaded files
    if newly_downloaded:
        print("\n Newly downloaded tables for analysis:")
        for filepath in newly_downloaded:
            print(f"\n File: {os.path.basename(filepath)}")
            df_preview = pd.read_csv(filepath, nrows=2)
            print(df_preview)
    else:
        print("\n No new tables were downloaded.")

if __name__ == "__main__":
    fetch_all_non_empty_tables()
