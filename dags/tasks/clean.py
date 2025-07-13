import pandas as pd
import re
from datetime import datetime, timedelta

def clean_data(df):
    if df.empty:
        return df

    def parse_date(text):
        if not isinstance(text, str):
            return pd.NaT

        text = text.lower()
        today = datetime.today().date()

        if 'today' in text:
            return today
        elif 'yesterday' in text:
            return today - timedelta(days=1)

        match = re.search(r'\d{2}/\d{2}/\d{4}', text)
        if match:
            return datetime.strptime(match.group(), '%d/%m/%Y').date()

        return pd.NaT

    df['DateLastUpdated'] = df['DateLastUpdated'].apply(parse_date)
    df['Price'] = df['Price'].astype(str).str.extract(r'(\d{1,3}(?:,\d{3})*(?:\.\d+)?)')[0]
    df['Price'] = df['Price'].str.replace(',', '').astype(float)
    df['Rooms'] = df['Rooms'].astype(str).str.extract(r'(\d+)').astype(pd.Int64Dtype())
    df['Link'] = df['Link'].apply(lambda x: f'https://rightmove.co.uk{x}' if pd.notnull(x) and x.startswith('/') else x)
    df.drop_duplicates(subset='Link', inplace=True)
    return df
