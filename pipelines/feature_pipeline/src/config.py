import os
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
    "name": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
}

HOPSWORKS_CONFIG = {
    "project": os.getenv("HOPSWORKS_PROJECT_NAME"),
    "api_key": os.getenv("HOPSWORKS_API_KEY"),
}
