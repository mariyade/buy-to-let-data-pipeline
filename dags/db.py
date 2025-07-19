import pandas as pd
from sqlalchemy import create_engine

DB_URL = 'postgresql+psycopg2://airflow:airflow@postgres:5432/airflow'

def get_connection():
    engine = create_engine(DB_URL)
    return engine.connect()

def save_to_db(df, table_name, if_exists='replace'):
    engine = create_engine(DB_URL)
    df.to_sql(table_name, engine, if_exists=if_exists, index=False)


def load_from_db(table_name):
    engine = create_engine(DB_URL)
    return pd.read_sql(f'SELECT * FROM {table_name}', engine)
