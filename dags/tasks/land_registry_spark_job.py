from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, upper, trim
import shutil
import glob
import os

def run_land_registry_summary():
    os.environ["HOME"] = "/tmp"

    spark = SparkSession.builder \
        .appName("Land Registry 2024 Summary") \
        .master("local[*]") \
        .config("spark.driver.extraJavaOptions", "-Duser.home=/tmp") \
        .getOrCreate()

    df = spark.read.csv("/opt/spark-data/pp-2024.csv", header=False).toDF(
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
