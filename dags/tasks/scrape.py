import requests
import time
from bs4 import BeautifulSoup
from urllib.parse import urlencode
import pandas as pd

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
}

def build_rightmove_url(filters: dict[str, str], channel: str = 'BUY') -> str:
    channel = channel.upper()
    action = 'sale' if channel == 'BUY' else 'rent'
    preposition = 'for' if channel == 'BUY' else 'to'
    return f'https://www.rightmove.co.uk/property-{preposition}-{action}/find.html' + '?' + urlencode(filters)

def get_total_results(url):
    response = requests.get(url, headers=HEADERS, timeout=10)
    soup = BeautifulSoup(response.text, 'html.parser')

    results_elem = soup.select_one('[class^="ResultsCount_resultsCount"] > p > span')
    if results_elem:
        try:
            total_results = int(results_elem.text.strip().replace(',', ''))
            print(f"Total results found: {total_results}")
            return total_results
        except ValueError:
            print("Could not parse total results number")
    return None

def scrape_listings(filters, max_pages, channel):
    url = build_rightmove_url(filters, channel=channel)
    total_results = get_total_results(url)
    if total_results is None or total_results == 0:
        print("No results found")
        return pd.DataFrame()

    pages = (total_results + 24) // 25
    pages_to_scrape = min(pages, max_pages)

    listings = []
    for page_num in range(pages_to_scrape):
        filters['index'] = page_num * 24
        url = build_rightmove_url(filters, channel=channel)
        response = requests.get(url, headers=HEADERS)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')

        result_cards = soup.select('[class$="propertyCard-details"]')
        if not result_cards:
            print(f"No results on page {page_num + 1}")
            break

        for card in result_cards:
            try:
                address_el = card.select_one('div[class^="PropertyAddress_address"]')
                price_el = card.select_one('div[class^="PropertyPrice_price"]')
                roomCount_el = card.select_one('span[class^="PropertyInformation_bedroomsCount"]')
                link_elem = card.select_one('a[data-testid="property-details-lozenge"]')
                date_el = card.select_one('span[class^="MarketedBy_addedOrReduced"]')

                address = address_el.text.strip() if address_el else None
                price = price_el.text.strip() if price_el else None
                room_count = roomCount_el.text.strip() if roomCount_el else None
                relative_link = link_elem['href'] if link_elem else None
                date_text = date_el.text.strip() if date_el else None

                listings.append({
                    'Address': address,
                    'Postcode': filters['searchLocation'],
                    'Price': price,
                    'Rooms': room_count,
                    'Link': relative_link,
                    'DateLastUpdated': date_text
                })
            except Exception:
                continue

        time.sleep(2)
    return pd.DataFrame(listings)
