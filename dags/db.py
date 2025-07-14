import sqlite3
import pandas as pd

DB_PATH = '/opt/airflow/dags/properties.db'

def get_connection():
    return sqlite3.connect(DB_PATH)

def save_to_db(df, table_name, if_exists='append'):
    with get_connection() as conn:
        df.to_sql(table_name, conn, if_exists=if_exists, index=False)

def load_from_db(table_name):
    with get_connection() as conn:
        return pd.read_sql(f'SELECT * FROM {table_name}', conn)
