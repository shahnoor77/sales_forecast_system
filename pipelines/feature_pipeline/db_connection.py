import logging
import psycopg2
from psycopg2 import sql


# Step 1: Setup logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    handlers=[
        logging.FileHandler("db_access.log"),
        logging.StreamHandler()
    ]
)

# Step 2: DB Configuration
# add credentials for your db connection
db_config = {
    "host": "your_host_id",
    "port": "port_number",
    "dbname": "your_db_name",
    "user": "your_user_name",
    "password": "your_password"
}


# Step 3: Connect & List Tables

try:
    logging.info("Connecting to the database...")
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()

    logging.info("Connected successfully ")

    # Query all table names from current schema (usually 'public')
    logging.info("Fetching table names...")
    cursor.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public'
        ORDER BY table_name;
    """)

    tables = cursor.fetchall()

    if not tables:
        logging.warning("No tables found in 'public' schema.")
    else:
        logging.info("Available tables:")
        for i, table in enumerate(tables, start=1):
            print(f"{i}. {table[0]}")
    
    cursor.close()
    conn.close()
    logging.info("Connection closed.")

except Exception as e:
    logging.error(f"Error connecting to database: {e}")
