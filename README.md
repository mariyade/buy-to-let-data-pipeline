# Buy-to-Let Property Analytics Data Pipeline

End-to-end data pipeline to automate the scraping, processing, and analysis of buy-to-let property listings across key UK cities.

<img width="628" height="448" alt="Screenshot 2025-07-18 at 00 23 07" src="https://github.com/user-attachments/assets/af97baef-492b-46cb-95db-31cc2279647a" />


---

## Background

This project is a proof of concept (PoC) for automating the analysis of buy-to-let property investment returns across major UK cities. It identifies properties with high rental yield based on scraped listings from Rightmove and calculates both gross and net yields.

Built using **Python**, **Apache Airflow**, and **Docker**, the pipeline is modular and configurable, allowing users to specify:
- Number of bedrooms  
- Maximum property prices  
- Search radius
- Property type
- Postcodes (via a CSV)

---

## Scope

The PoC focuses on 5 major UK cities:

- London  
- Manchester  
- Birmingham  
- Liverpool  
- Bristol

For each city, 3 central postcodes were selected. These are defined in `data/postcode_location_map.csv` and can be updated. Filters such as bedrooms and radius are set in `dags/config/config_filters.py`.

---

## Technology

- **Language:** Python  
- **Workflow Orchestration:** Apache Airflow  
- **Containerization:** Docker, Docker Compose  
- **Database:** SQLite (for PoC, can be swapped for BigQuery/Postgres)  
- **Visualization:** Google Looker Studio, Matplotlib, Seaborn  

---

## Data Sources

- **Sale Listings:** Rightmove (scraped by postcode and Rightmove-specific `LocationIdentifier`)
- **Rental Listings:** Rightmove (scraped by postcode and room count)
- **Stamp Duty:** Based on embedded UK tiered rules

---

## Airflow DAG Data Pipeline Tasks

### 1. scrape_sale_listings
- Loads postcodes from `data/postcode_location_map.csv`
- Applies filters from `config_filters.py`
- Scrapes sale listings from Rightmove
- Saves results to SQLite table: `raw_sale_listings`

### 2. scrape_rent_listings
- Same as above but for rental listings
- Saves to: `raw_rent_listings`

### 3. clean_listings
- Cleans raw sale and rent data using `clean_data()`
- Saves cleaned versions to `buy_listings` and `rent_listings`

### 4. aggregate_and_calculate
- Joins listings with average rent (by postcode + room)
- Calculates:
  - EstimatedAnnualRent
  - Gross_Yield_%
  - Net_Yield_%
- Saves output to `buy_listings_with_yields.csv` and DB
- Logs:
  - Top 20 properties by Net Yield
  - Summary of prices per room and postcode

### 5. visualize_net_yield
- Generates a bar chart:
  - **X-axis**: Postcode  
  - **Y-axis**: Avg Net Yield (%)  
  - **Hue**: Room count  
- Saves figure to `net_yield_by_postcode.png`

---

## Storage

All data is stored in a local SQLite database:
- `raw_sale_listings`, `raw_rent_listings`
- `buy_listings`, `rent_listings`
- `buy_listings_with_yields`

### Example Output: Successful Execution

To see what a successful pipeline run looks like, here's a screenshot of the Airflow DAG after successful execution:

<img width="915" height="472" alt="dag" src="https://github.com/user-attachments/assets/cd37279a-ee53-40c3-8b86-088ab1fff582" />

At the end of a successful run, the pipeline produces a CSV file named:

```text
buy_listings_with_yields.csv
```

This file is saved locally and contains cleaned, processed, and yield-calculated buy-to-let property listings with the following columns:

- Address
- Postcode
- Price
- Rooms
- Link
- DateLastUpdated
- EstimatedAnnualRent
- Gross_Yield_%
- Net_Yield_%

### Example Dashboard (Looker Studio)

Google Looker Studio was used to experiment with visualising the final dataset.

The screenshot below shows example insights from a successful run:
<img width="828" height="648" alt="Screenshot 2025-07-18 at 00 23 07" src="https://github.com/user-attachments/assets/8b7cbff6-2edf-465f-8c47-02c28d7fbead" />


