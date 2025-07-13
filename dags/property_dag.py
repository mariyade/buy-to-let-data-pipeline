from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import sqlite3
import time

from tasks import scrape, clean, yield_calculations

def run_scrape(**context):
    postcode_map = pd.read_csv('/opt/airflow/data/postcode_location_map.csv')

    conn = sqlite3.connect('/opt/airflow/data/properties.db')

    for idx, row in postcode_map.iterrows():
        postcode = row['Postcode']
        location_id = row['LocationIdentifier']

        filterSale = {
            'searchLocation': postcode,
            'useLocationIdentifier': 'true',
            'locationIdentifier': location_id,
            'radius': 0.25,
            'minPrice': 200000,
            'maxPrice': 900000,
            'minBedrooms': 1,
            'maxBedrooms': 1,
            'propertyTypes': 'flat',
            '_includeSSTC': 'on',
            'dontShow': 'newHome,retirement,sharedOwnership,auction',
            'sortType': 6,
            'channel': 'BUY',
            'transactionType': 'BUY',
            'displayLocationIdentifier': 'undefined'
        }

        filterRent = {
            'searchLocation': postcode,
            'useLocationIdentifier': 'true',
            'locationIdentifier': location_id,
            'radius': 0.25,
            'minBedrooms': 1,
            'maxBedrooms': 1,
            'includeLetAgreed': 'on',
            'dontShow': 'retirement,student,houseShare',
            'sortType': 6,
            'channel': 'RENT',
            'transactionType': 'LETTING',
            'displayLocationIdentifier': 'undefined'
        }

        df_buy = scrape.scrape_listings(filterSale, max_pages=1, channel='BUY')
        df_rent = scrape.scrape_listings(filterRent, max_pages=1, channel='RENT')

        clean_df_buy = clean.clean_data(df_buy)
        clean_df_rent = clean.clean_data(df_rent)

        if not clean_df_buy.empty:
            clean_df_buy.to_sql('buy_listings', conn, if_exists='append', index=False)
        if not clean_df_rent.empty:
            clean_df_rent.to_sql('rent_listings', conn, if_exists='append', index=False)

        time.sleep(5)

    conn.close()

def run_calculate_yield(**context):
    conn = sqlite3.connect('/opt/airflow/data/properties.db')

    df_buy_all = pd.read_sql('SELECT * FROM buy_listings', conn)
    df_rent_all = pd.read_sql('SELECT * FROM rent_listings', conn)

    avg_rent_per_postcode = df_rent_all.groupby('Postcode')['Price'].mean().to_dict()

    df_buy_all = yield_calculations.calculate_gross_yield_all(df_buy_all, avg_rent_per_postcode)
    df_buy_all = yield_calculations.calculate_net_yield(df_buy_all)

    df_buy_all.to_csv('/opt/airflow/data/buy_listings_with_yields.csv', index=False)
    df_buy_all.to_sql('buy_listings_with_yields', conn, if_exists='replace', index=False)

    conn.close()

with DAG(
    dag_id="property_investment_pipeline",
    start_date=datetime(2025, 7, 13),
    schedule_interval="@daily",
    catchup=False,
    tags=["buy-to-let", "real-estate"],
) as dag:

    scrape_task = PythonOperator(
        task_id="scrape_listings",
        python_callable=run_scrape,
    )

    yield_task = PythonOperator(
        task_id="calculate_yield",
        python_callable=run_calculate_yield,
    )

    scrape_task >> yield_task
