import os

import psycopg2
import sqlalchemy as sa


USER = 'jupyter'
PASSWORD = os.environ['POSTGRES_PASS']
HOST = 'localhost'
PORT = '5432'
DB = 'expunge'

DATABASE_URI = f"postgresql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DB}"


def get_db_engine(echo: bool = False):
    return sa.create_engine(DATABASE_URI, echo=echo)


def get_psycopg2_conn():
    return psycopg2.connect(
        user=USER,
        password=PASSWORD,
        host=HOST,
        port=PORT,
        dbname=DB
    )
