import pandas as pd

def calculate_net_yield(df_buy, void_rate=0.05, annual_maintenance_rate=0.01, management_fee_rate=0.10, mortgage_rate=0.0515, ltv=0.75, verbose=True):
    if df_buy.empty or 'EstimatedAnnualRent' not in df_buy.columns:
        print("Missing data or gross yield not calculated")
        return df_buy

    df = df_buy.copy()
    rent_after_voids = df['EstimatedAnnualRent'] * (1 - void_rate)
    maintenance_cost = df['Price'] * annual_maintenance_rate
    management_cost = rent_after_voids * management_fee_rate
    mortgage_interest = df['Price'] * ltv * mortgage_rate

    net_income = rent_after_voids - maintenance_cost - management_cost - mortgage_interest
    df['Net_Yield_%'] = (net_income / df['Price']) * 100

    if verbose:
        print("\nTop properties by Net Yield:")
        print(df[['Address', 'Price', 'Net_Yield_%']].sort_values(by='Net_Yield_%', ascending=False).head(5))

    return df

def calculate_gross_yield_all(df_buy, avg_rent_per_postcode, verbose=True):
    def get_annual_rent(postcode):
        avg_monthly = avg_rent_per_postcode.get(postcode, 0)  # Fallback if no rent data
        return avg_monthly * 12

    df_buy['EstimatedAnnualRent'] = df_buy['Postcode'].apply(get_annual_rent)
    df_buy['Gross_Yield_%'] = (df_buy['EstimatedAnnualRent'] / df_buy['Price']) * 100

    if verbose:
        print("\nTop properties by Gross Yield (all areas):")
        print(df_buy[['Postcode', 'Address', 'Price', 'Gross_Yield_%']].sort_values('Gross_Yield_%', ascending=False).head(5))

    return df_buy

def calculate_stamp_duty(price, is_buy_to_let=True):
    surcharge = 0.03 * price if is_buy_to_let else 0
    base = 0
    if price > 250000:
        base += 0.05 * min(price - 250000, 675000)
    if price > 925000:
        base += 0.10 * min(price - 925000, 575000)
    if price > 1500000:
        base += 0.12 * (price - 1500000)

    return base + surcharge
