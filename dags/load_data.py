from utils.db import load_from_db

def load_listings():
    df_buy_all = load_from_db('buy_listings')
    df_rent_all = load_from_db('rent_listings')
    return df_buy_all, df_rent_all