# Buy-to-Let Property Analytics Data Pipeline

End-to-end data pipeline to automate the scraping, processing, and analysis of buy-to-let property listings across key UK cities.

<img width="628" height="448" alt="Screenshot 2025-07-18 at 00 23 07" src="https://github.com/user-attachments/assets/af97baef-492b-46cb-95db-31cc2279647a" />


---

### Background

This project is a proof of concept (PoC) for automating the analysis of buy-to-let property investment returns. It scrapes listings from Rightmove, calculates gross and net yields, and surfaces the most attractive properties based on yield.

---

### Scope

The PoC focuses on 5 major UK cities:

- London  
- Manchester  
- Birmingham  
- Liverpool  
- Bristol

For each city, 3 central postcodes were selected. These are defined in `data/postcode_location_map.csv` and can be updated. Filters such as bedrooms, maximum property prices, and radius can be condfigured in `dags/config/config_filters.py`. 

---

### Technology

- **Language:** Python  
- **Containerization:** Docker, Docker Compose  
- **Workflow Orchestration:** Apache Airflow  
- **Database:** PostgreSQL
- **Cloud storage:** Google Cloud Storage (GCS)
- **Cloud Data Warehouse:** Google BigQuery
- **Visualization:** Google Looker Studio, Matplotlib, Seaborn  

---

### Data Sources

- **Sale Listings:** Rightmove (scraped by postcode and Rightmove-specific `LocationIdentifier`)
- **Rental Listings:** Rightmove (scraped by postcode and room count)
- **Stamp Duty:** Based on embedded UK tiered rules

---

### Outputs and Visuals

- CSV: `/opt/airflow/output/top_20_yield.csv` (top 20 by net yield)
- PNG: `net_yield_by_postcode.png` (bar chart of average net yield by postcode and room count)

  
<img width="800" height="300" alt="net_yield_by_postcode" src="https://github.com/user-attachments/assets/2f506de2-7ec6-4271-b486-7af61f68e9cb" />

  
- Upload to **Google Cloud Storage** via `upload_to_gcs()`
- Load into **BigQuery** via `load_csv_to_bigquery()`

---
### Data Storage

All data is stored in PostgreSQL:

| Table                         | Description                           |
|------------------------------|---------------------------------------|
| `raw_sale_listings`          | Uncleaned sale listings               |
| `raw_rent_listings`          | Uncleaned rental listings             |
| `buy_listings`               | Cleaned sale listings                 |
| `rent_listings`              | Cleaned rental listings               |
| `buy_listings_with_yields`   | Final enriched dataset with gross/net yields |

---

### Example Dashboard (Looker Studio)

Google Looker Studio was used to experiment with visualising the final dataset.

The screenshot below shows example insights from a successful run:
<img width="828" height="648" alt="Screenshot 2025-07-18 at 00 23 07" src="https://github.com/user-attachments/assets/8b7cbff6-2edf-465f-8c47-02c28d7fbead" />
