from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import time
import sys
import os

from db import save_to_db, load_from_db
from scraping import scrape_listings
from config.config_filters import filterSale, filterRent, scrapeParam
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

def scrape_sale_listings(**kwargs):
    postcode_map = pd.read_csv(os.path.join(os.path.dirname(__file__), 'data', 'postcode_location_map.csv'))
    
    all_buy = [] 

    for _, row in postcode_map.iterrows():
        postcode = row['Postcode']
        location_id = row['LocationIdentifier']

        buy_filters = filterSale.copy()
        buy_filters.update({'searchLocation': postcode, 'locationIdentifier': location_id})

        df_buy = scrape_listings(buy_filters, scrapeParam['max_pages'], channel='BUY')

        if not df_buy.empty:
            all_buy.append(df_buy)  
        time.sleep(5)

    if all_buy:
        df_all = pd.concat(all_buy, ignore_index=True)
        save_to_db(df_all, 'raw_sale_listings', if_exists='replace') 

def scrape_rent_listings(**kwargs):
    postcode_map = pd.read_csv(os.path.join(os.path.dirname(__file__), 'data', 'postcode_location_map.csv'))

    all_rent = []  

    for _, row in postcode_map.iterrows():
        postcode = row['Postcode']
        location_id = row['LocationIdentifier']

        rent_filters = filterRent.copy()
        rent_filters.update({'searchLocation': postcode, 'locationIdentifier': location_id})
        df_rent = scrape_listings(rent_filters, scrapeParam['max_pages'], channel='RENT')

        if not df_rent.empty:
            all_rent.append(df_rent)  
        time.sleep(5)

    if all_rent:
        df_all = pd.concat(all_rent, ignore_index=True)
        save_to_db(df_all, 'raw_rent_listings', if_exists='replace')

def clean_listings(**kwargs):
    df_raw_buy = load_from_db('raw_sale_listings')
    df_raw_rent = load_from_db('raw_rent_listings')

    clean_df_buy = clean_data(df_raw_buy)
    clean_df_rent = clean_data(df_raw_rent)

    if not clean_df_buy.empty:
        save_to_db(clean_df_buy, 'buy_listings', if_exists='replace')
    if not clean_df_rent.empty:
        save_to_db(clean_df_rent, 'rent_listings', if_exists='replace')

def aggregate_and_calculate(**kwargs):
    df_buy_all = load_from_db('buy_listings')
    df_rent_all = load_from_db('rent_listings')

    avg_rent_per_postcode = df_rent_all.groupby('Postcode')['Price'].mean().to_dict()

    df_buy_all = calculate_gross_yield_all(df_buy_all, avg_rent_per_postcode, verbose=True)
    df_buy_all = calculate_net_yield(df_buy_all, verbose=True)

    df_buy_all.to_csv('buy_listings_with_yields.csv', index=True)
    save_to_db(df_buy_all, 'buy_listings_with_yields', if_exists='replace')

    top_20_yield = df_buy_all.sort_values('Net_Yield_%', ascending=False).head(20)
    print("\nTop 20 Properties by Net Yield:")
    print(top_20_yield[['Postcode', 'Net_Yield_%', 'Price', 'Rooms', 'Address', 'Link']])

    yield_by_postcode = df_buy_all.groupby('Postcode').agg({
        'Net_Yield_%': 'mean',
        'Price': 'mean'
    }).reset_index().sort_values('Net_Yield_%', ascending=False)

    print("\nAverage Net Yield by Postcode:")
    print(yield_by_postcode)

def visualize_net_yield(**kwargs):
    df_buy_all = load_from_db('buy_listings_with_yields')

    yield_by_postcode = df_buy_all.groupby('Postcode').agg({
        'Net_Yield_%': 'mean',
        'Price': 'mean'
    }).reset_index().sort_values('Net_Yield_%', ascending=False)

    plt.figure(figsize=(10, 6))
    sns.barplot(data=yield_by_postcode, x='Postcode', y='Net_Yield_%', palette='Blues_d')

    plt.title('Average Net Yield by Postcode')
    plt.xlabel('Postcode')
    plt.ylabel('Net Yield (%)')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig('net_yield_by_postcode.png')
    print("Bar chart saved to net_yield_by_postcode.png")

scrape_sale_task = PythonOperator(
    task_id='scrape_sale_listings',
    python_callable=scrape_sale_listings,
    provide_context=True,
    dag=dag,
)

scrape_rent_task = PythonOperator(
    task_id='scrape_rent_listings',
    python_callable=scrape_rent_listings,
    provide_context=True,
    dag=dag,
)

clean_task = PythonOperator(
    task_id='clean_listings',
    python_callable=clean_listings,
    provide_context=True,
    dag=dag,
)

aggregate_task = PythonOperator(
    task_id='aggregate_and_calculate',
    python_callable=aggregate_and_calculate,
    provide_context=True,
    dag=dag,
)

visualize_net_task = PythonOperator(
    task_id='visualize_net_yield',
    python_callable=visualize_net_yield,
    provide_context=True,
    dag=dag,
)

scrape_sale_task >> scrape_rent_task >> clean_task >> aggregate_task >> visualize_net_task
