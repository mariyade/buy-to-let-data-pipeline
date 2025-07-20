
variable "credentials" {
  description = "Path to your Google Cloud service account key file"
  default     = "./keys/my-creds.json"
}

variable "project" {
  description = "Google Cloud project ID"
  type        = string
  default     = "soy-geography-466420-u3"
}

variable "region" {
  description = "Google Cloud region"
  default     = "europe-west2"
}

variable "location" {
  description = "BigQuery and GCS location"
  default     = "EU"
}

variable "gcs_bucket_name" {
  description = "Google Cloud Storage bucket name"
  default     = "buy-to-let-uk-gcp-2025"
}

variable "bq_dataset_name" {
  description = "BigQuery dataset name"
  default     = "buy_to_let_data_2025"
}
