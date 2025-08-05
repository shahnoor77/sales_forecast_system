import os
import pandas as pd
import logging
from sqlalchemy import create_engine, inspect
from urllib.parse import quote_plus
from src.config import DB_CONFIG

logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s')
logger = logging.getLogger(__name__)

OUTPUT_DIR = os.path.join("data", "raw")
os.makedirs(OUTPUT_DIR, exist_ok=True)

def get_engine():
    encoded_pw = quote_plus(DB_CONFIG['password'])
    url = f"mysql+pymysql://{DB_CONFIG['user']}:{encoded_pw}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['name']}"
    return create_engine(url)

def fetch_all_tables():
    engine = get_engine()
    inspector = inspect(engine)
    tables = inspector.get_table_names()

    for table in tables:
        out_file = os.path.join(OUTPUT_DIR, f"{table}.csv")
        if os.path.exists(out_file):
            logger.info(f"Skipping already downloaded table: {table}")
            continue
        try:
            df = pd.read_sql_query(f"SELECT * FROM `{table}`", engine)
            if df.empty:
                logger.info(f"Skipping empty table: {table}")
                continue
            df.to_csv(out_file, index=False)
            logger.info(f"Saved {len(df)} rows from {table} to {out_file}")
        except Exception as e:
            logger.error(f"Error fetching table {table}: {e}")