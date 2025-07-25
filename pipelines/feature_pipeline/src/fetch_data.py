import os
import pandas as pd
import logging
from datetime import datetime, timedelta
from sqlalchemy import create_engine, inspect
from dotenv import load_dotenv
from urllib.parse import quote_plus

# Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load credentials
load_dotenv()
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

# Create engine
encoded_password = quote_plus(DB_PASSWORD)
DATABASE_URL = f"mysql+pymysql://{DB_USER}:{encoded_password}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DATABASE_URL)

# Target directory
OUTPUT_DIR = "cleaned_tables"
os.makedirs(OUTPUT_DIR, exist_ok=True)

def fetch_and_clean_tables(days=90):
    """Fetch tables with non-empty rows and time-based data."""
    inspector = inspect(engine)
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=days)

    for table_name in inspector.get_table_names():
        logger.info(f"Checking table: {table_name}")

        try:
            # Get sample of table to inspect date/time column
            df_sample = pd.read_sql_query(f"SELECT * FROM {table_name} LIMIT 5", engine)

            # Skip empty tables
            if df_sample.empty:
                logger.info(f"Skipping empty table: {table_name}")
                continue

            # Try to find a date/datetime column
            date_columns = [col for col in df_sample.columns if "date" in col.lower() or "time" in col.lower()]
            if not date_columns:
                logger.warning(f"No date/time column found in {table_name}, skipping.")
                continue

            date_col = date_columns[0]  # Pick first matched column
            query = f"""
                SELECT * FROM {table_name}
                WHERE `{date_col}` BETWEEN '{start_date}' AND '{end_date}'
            """

            df = pd.read_sql_query(query, engine)

            if df.empty:
                logger.info(f"No recent data in {table_name}, skipping.")
                continue

            # Save as both CSV and Parquet
            csv_path = os.path.join(OUTPUT_DIR, f"{table_name}.csv")
            parquet_path = os.path.join(OUTPUT_DIR, f"{table_name}.parquet")

            df.to_csv(csv_path, index=False)
            df.to_parquet(parquet_path, index=False)
            logger.info(f"Saved table {table_name}: {len(df)} rows.")
        
        except Exception as e:
            logger.error(f"Error processing table {table_name}: {e}")

if __name__ == "__main__":
    fetch_and_clean_tables()
