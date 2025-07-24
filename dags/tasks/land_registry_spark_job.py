from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, upper, trim
import shutil
import glob
import os
import requests

def run_land_registry_summary():
    os.environ["HOME"] = "/tmp"

    # Land Registry Price Paid Data (2024 full-year CSV): https://www.gov.uk/government/statistical-data-sets/price-paid-data-downloads
    # Note: The download URL may change each year. For future updates, check the above source for the latest file.
    download_url = "http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-2024.csv"
    local_path = "/opt/spark-data/pp-2024.csv"

    if not os.path.exists(local_path):
        response = requests.get(download_url, stream=True)
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        with open(local_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)

    spark = SparkSession.builder \
        .appName("Land Registry 2024 Summary") \
        .master("local[*]") \
        .config("spark.driver.extraJavaOptions", "-Duser.home=/tmp") \
        .getOrCreate()

    df = spark.read.csv(local_path, header=False).toDF(
        "transaction_id", "price", "date_of_transfer", "postcode", "property_type",
        "new_build", "tenure", "paon", "saon", "street", "locality",
        "town_city", "district", "county", "ppd_category", "status"
    )

    df = df.withColumn("postcode", trim(upper(col("postcode"))))
    df = df.withColumn("outcode", split(col("postcode"), " ").getItem(0))

    postcode_df = spark.read.csv("/opt/spark-dags-data/postcode_location_map.csv", header=False) \
        .withColumnRenamed("_c0", "outcode")

    target_outcodes = [row["outcode"] for row in postcode_df.select("outcode").distinct().collect()]

    df = df.withColumn("price", col("price").cast("int")) \
           .filter(col("price").isNotNull()) \
           .filter(col("outcode").isin(target_outcodes))

    summary = df.groupBy("outcode").avg("price") \
                .withColumnRenamed("avg(price)", "Avg_Sold_Price_2024")

    output_dir = "/opt/airflow/output/output_2024"
    final_csv_path = "/opt/airflow/output/avg_sold_price_2024.csv"

    summary.coalesce(1).write.mode("overwrite").option("header", True).csv(output_dir)

    try:
        csv_file = glob.glob(os.path.join(output_dir, "part-*.csv"))[0]
        shutil.copy(csv_file, final_csv_path)
    except Exception:
        pass

    spark.stop()
