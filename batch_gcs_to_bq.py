# Import DAG class to define an Airflow DAG
from airflow import DAG

# Operator to run BigQuery jobs (SQL queries, validation, etc.)
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

# Operator to load data directly from GCS into BigQuery
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

# Utility to create start_date easily
from airflow.utils.dates import days_ago

# Used for retry delay configuration
from datetime import timedelta

# Default arguments applied to all tasks in this DAG
default_args = {
    "owner": "data-engineering",          # Owner of the DAG (for visibility)
    "depends_on_past": False,              # Do not wait for previous run to succeed
    "retries": 2,                          # Number of retries on failure
    "retry_delay": timedelta(minutes=5),  # Wait time between retries
}

# Define the DAG using context manager
with DAG(
    dag_id="batch_gcs_to_bq",              # Unique DAG name
    default_args=default_args,             # Apply default args to all tasks
    description="Batch load data from GCS to BigQuery",
    schedule_interval="0 2 * * *",         # Run daily at 2 AM
    start_date=days_ago(1),                # Start from yesterday
    catchup=False,                         # Do not backfill missed runs
    tags=["batch", "gcs", "bigquery"],     # Tags for UI grouping
) as dag:

    # Task to load CSV files from GCS into BigQuery table
    load_gcs_to_bq = GCSToBigQueryOperator(
        task_id="load_gcs_to_bq",           # Unique task ID
        bucket="source-data-bucket",        # GCS bucket name
        source_objects=["sales/daily/*.csv"],  # File path pattern in GCS
        destination_project_dataset_table=(
            "project_id.dataset_id.sales"   # Target BigQuery table
        ),
        source_format="CSV",                # Input file format
        skip_leading_rows=1,                # Skip CSV header row
        write_disposition="WRITE_APPEND",   # Append new data
        autodetect=True,                    # Let BQ infer schema
    )

    # Task to validate whether today's data is loaded correctly
    validate_load = BigQueryInsertJobOperator(
        task_id="validate_load",             # Validation task ID
        configuration={
            "query": {
                "query": """
                    SELECT COUNT(*) 
                    FROM `project_id.dataset_id.sales`
                    WHERE DATE(load_timestamp) = CURRENT_DATE()
                """,                         # SQL validation query
                "useLegacySql": False,       # Use Standard SQL
            }
        },
    )

    # Define task dependency:
    # Validation runs only after data load is successful
    load_gcs_to_bq >> validate_load
