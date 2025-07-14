from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
import time
import sys
import os

from db import save_to_db, load_from_db
from rightmove_scraper import scrape_listings, DEFAULT_BUY_FILTERS, DEFAULT_RENT_FILTERS
from cleaner import clean_data
from yield_calculator import calculate_gross_yield_all, calculate_net_yield

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 7, 13),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'property_scraping_pipeline',
    default_args=default_args,
    description='DAG for scraping and analyzing property listings',
    schedule_interval=timedelta(days=1), 
    catchup=False,
)

def scrape_all_postcodes(**kwargs):
    postcode_map = pd.read_csv(os.path.join(os.path.dirname(__file__), '..', 'data', 'postcode_location_map.csv'))
    
    for idx, row in postcode_map.iterrows():
        postcode = row['Postcode']
        location_id = row['LocationIdentifier']

        buy_filters = DEFAULT_BUY_FILTERS.copy()
        buy_filters.update({'searchLocation': postcode, 'locationIdentifier': location_id})

        rent_filters = DEFAULT_RENT_FILTERS.copy()
        rent_filters.update({'searchLocation': postcode, 'locationIdentifier': location_id})

        df_buy = scrape_listings(buy_filters, max_pages=1, channel='BUY')
        df_rent = scrape_listings(rent_filters, max_pages=1, channel='RENT')

        clean_df_buy = clean_data(df_buy)
        clean_df_rent = clean_data(df_rent)

        if not clean_df_buy.empty:
            save_to_db(clean_df_buy, 'buy_listings')
        if not clean_df_rent.empty:
            save_to_db(clean_df_rent, 'rent_listings')

        time.sleep(5)

scrape_task = PythonOperator(
    task_id='scrape_all_postcodes',
    python_callable=scrape_all_postcodes,
    provide_context=True,
    dag=dag,
)

def aggregate_and_calculate(**kwargs):
    df_buy_all = load_from_db('buy_listings')
    df_rent_all = load_from_db('rent_listings')

    avg_rent_per_postcode = df_rent_all.groupby('Postcode')['Price'].mean().to_dict()

    df_buy_all = calculate_gross_yield_all(df_buy_all, avg_rent_per_postcode, verbose=False)
    df_buy_all = calculate_net_yield(df_buy_all, verbose=False)

    df_buy_all.to_csv('buy_listings_with_yields.csv', index=False)
    save_to_db(df_buy_all, 'buy_listings_with_yields', if_exists='replace')

    yield_by_postcode = df_buy_all.groupby('Postcode').agg({
        'Net_Yield_%': 'mean',
        'Price': 'mean'
    }).reset_index().sort_values('Net_Yield_%', ascending=False)

    print("Average Net Yield by Postcode:")
    print(yield_by_postcode)

aggregate_task = PythonOperator(
    task_id='aggregate_and_calculate',
    python_callable=aggregate_and_calculate,
    provide_context=True,
    dag=dag,
)

scrape_task >> aggregate_task
