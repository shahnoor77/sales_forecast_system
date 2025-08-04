import pandas as pd
from sqlalchemy import create_engine
from urllib.parse import quote_plus
from datetime import datetime
from src.config import DB_CONFIG

def get_engine():
    encoded_pw = quote_plus(DB_CONFIG['password'])
    url = f"mysql+pymysql://{DB_CONFIG['user']}:{encoded_pw}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['name']}"
    return create_engine(url)

def fetch_daily_data(last_date: str) -> pd.DataFrame:
    query = f"""
        SELECT * FROM orders
        WHERE created_at > '{last_date}' AND created_at <= '{datetime.today().date()}'
    """
    engine = get_engine()
    df = pd.read_sql_query(query, engine)
    return df.drop_duplicates()