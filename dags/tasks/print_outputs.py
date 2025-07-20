import pandas as pd

def print_top_20_by_net_yield(df):
    print("\nTop 20 Properties by Net Yield:")
    top_20 = df.sort_values('Net_Yield_%', ascending=False).head(20)
    print(top_20[['Postcode', 'Net_Yield_%', 'Price', 'Rooms', 'Address', 'Link']])

def print_price_summary(df_buy, df_rent):
    buy_summary = (
        df_buy.groupby(['Postcode', 'Rooms'])['Price']
        .mean().reset_index().rename(columns={'Price': 'Avg_Buy_Price'})
    )
    rent_summary = (
        df_rent.groupby(['Postcode', 'Rooms'])['Price']
        .mean().reset_index().rename(columns={'Price': 'Avg_Rent_Price'})
    )

    merged = pd.merge(buy_summary, rent_summary, on=['Postcode', 'Rooms'], how='outer')
    merged = merged.sort_values(['Postcode', 'Rooms'])

    print("\nAverage Buy & Rent Prices by Postcode and Rooms:")
    print(merged)
