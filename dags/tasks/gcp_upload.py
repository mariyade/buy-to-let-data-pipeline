from google.cloud import storage, bigquery
import os

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/opt/airflow/terraform/keys/my-creds.json"

def upload_to_gcs(bucket_name, source_file_path, destination_blob_name):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_path)
    print(f"Uploaded {source_file_path} to gs://{bucket_name}/{destination_blob_name}")

def load_csv_to_bigquery(bucket_name, source_blob_name, dataset_id, table_id):
    client = bigquery.Client()
    table_ref = f"{client.project}.{dataset_id}.{table_id}"
    uri = f"gs://{bucket_name}/{source_blob_name}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True,
        write_disposition="WRITE_TRUNCATE",
    )

    load_job = client.load_table_from_uri(uri, table_ref, job_config=job_config)
    load_job.result()  
    print(f"Loaded data into BigQuery: {table_ref}")