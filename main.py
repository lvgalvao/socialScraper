import datetime as dt
from google.cloud.sql.connector import Connector
import requests
import sqlalchemy
from time import sleep
import pandas as pd


import config

URL = 'https://social-scraper.ekz.com.br/hashtags/top/views'
INSTANCE_CONNECTION_NAME = config.INSTANCE_CONNECTION_NAME
DB_USER = config.DB_USER
DB_PASS = config.DB_PASS
DB_NAME = config.DB_NAME
TIME = 60

# initialize Connector object
connector = Connector()

# function to return the database connection object
def getconn():
    conn = connector.connect(
        INSTANCE_CONNECTION_NAME,
        "pymysql",
        user=DB_USER,
        password=DB_PASS,
        db=DB_NAME
    )
    return conn

# create connection pool with 'creator' argument to our connection object function
pool = sqlalchemy.create_engine(
    "mysql+pymysql://",
    creator=getconn,
)
# connect to connection pool
with pool.connect() as db_conn:
# create collected table in our tiktok database
    while True:
        # create table
        db_conn.execute(
        "CREATE TABLE IF NOT EXISTS final "
        "(id VARCHAR(255), name VARCHAR(255), "
        "vi BIGINT, "
        "parent VARCHAR(255), "
        "videos BIGINT, views BIGINT,"
        "timestamp TIMESTAMP);"
        )
        response = requests.get(URL)
        if response.content : print(f'ok info views')
        item = pd.json_normalize(response.json())

        # some transformation
        item.rename(columns = {'_id':'id'}, inplace = True)
        item.rename(columns = {'__v':'vi'}, inplace = True)
        item = item.drop(columns='desc', axis=1)
        item['timestamp'] = dt.datetime.now()

        # inserting date into sql
        with db_conn.connect().execution_options(autocommit=True) as conn:
            item.to_sql('final', con=conn, if_exists='append', index= False)

        print("Coleta")
        sleep(TIME)
