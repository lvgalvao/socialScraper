import json
import datetime as dt
from time import sleep
import requests
import pandas as pd
from google.cloud.sql.connector import Connector
import sqlalchemy

URL = 'https://social-scraper.ekz.com.br/hashtags/top/views'
key='AIzaSyAoq1ze9tw4DXyU4QjyYT9_c2UptZWfsDg'
INSTANCE_CONNECTION_NAME = 'bigquerycoursedemo-347312:us-central1:socialscraping'
print(f"Your instance connection name is: {INSTANCE_CONNECTION_NAME}")
DB_USER = "socialscraper"
DB_PASS = "&QfDp&XibOJ9~\Zm"
DB_NAME = "top_views"

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
  # create ratings table in our movies database

    while True:

        db_conn.execute(
        "CREATE TABLE IF NOT EXISTS topviews_prod "
        "(id VARCHAR(255), name VARCHAR(255), "
        "vi INTEGER, Idesc VARCHAR(255), "
        "parent VARCHAR(255), "
        "videos INTEGER, views INTEGER,"
        "timest TIMESTAMP);"
        )
        response = requests.get(URL)
        if response.content : print(f'ok info views')
        data = response.json() 
        item = pd.json_normalize(data)
        # item['timestamp'] = dt.datetime.now()
        TIME_now = dt.datetime.now()
        insert_stmt = sqlalchemy.text(
        "INSERT INTO topviews_prod (id, name, vi, Idesc, parent, videos, views, timest) VALUES (:id, :name, :vi, :Idesc, :parent, :videos, :views, :timest)",
        )
        ITEMID = item["_id"]
        ITEMNAME = item["name"]
        ITEM_V = item["__v"]
        ITEMDESC = item["desc"]
        ITEMPARENT = item["parent"]
        ITENVIDEOS = item["videos"]
        ITENVIEWS = item["views"]
        db_conn.execute(insert_stmt, id=ITEMID, name=ITEMNAME, v=ITEM_V, Idesc=ITEMDESC, parent=ITEMPARENT, videos=ITENVIDEOS, views=ITENVIEWS, timest=TIME_now), 
        print("Coleta")
        sleep(1)


# while True:
#     for path in ['related/brasileirao/views', 'related/brasileirao/videos', 'top/views', 'top/videos']:
#         URL = f'https://social-scraper.ekz.com.br/hashtags/{path}'
#         response = requests.get(URL)
#         if response.content : print(f'ok info {path}')
#         data = response.json() 
#         dfItem = pd.json_normalize(data)
#         print(dfItem)
#         sleep(1)