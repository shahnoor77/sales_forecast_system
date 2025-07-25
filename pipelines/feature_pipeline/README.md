This microservice is designed to implement feature pipeline.

In this part of system we will perform these tasks:
1- Fetch historical data from sql file or from database using query.
2- Cleaning data
3- Store data.

Requirements:

Database access or Historical data file.


we will also fetch live data for real-time-inference.

                                ***PRACTICING***

import sqlite3
from dotenv import load_env
import pandas as pd
from sqlalchemy import create_engine
import logger


#describe env variables

db_name = os.env("DB_name")
.
.
.
.
.etc




describe engine

database_url = f"postgresq;://{database_user}......"
engine = create_engine(database_engine)



def fetch_data(
    days:int,
)->DataFrames:

end date = datetime.now().date()
start_date = end_date - timedelta(days = days)

query = f """ 
SELECT .....FROM...WHERE order_date BETWEEN {'start_date'} AND {'end_date'}

try:

   df = pd.read_sql_query(query, engine)
   logger.info(f'fetched {len(df)} rows from db')
   print(df.head())
   return df
except exception as e:
    logger.error(f'error while fetching data: e ")
    return pd.DataFrame

if __name__ = '__main__":

fetch_data(days=days)


                     ****end***





