"""
DAG NAME  : sqlserver_to_gcs_csv
PURPOSE   : Extract data from SQL Server and store as CSV in GCS
NOTE      : No PythonOperator used
"""

# ------------------------------------------------
# 1. IMPORT AIRFLOW CORE LIBRARIES
# ------------------------------------------------

from airflow import DAG
from datetime import datetime, timedelta

# ------------------------------------------------
# 2. IMPORT SQL SERVER → GCS OPERATOR
# ------------------------------------------------

from airflow.providers.google.cloud.transfers.mssql_to_gcs import MsSqlToGCSOperator

# ------------------------------------------------
# 3. DEFAULT DAG ARGUMENTS
# ------------------------------------------------

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

# ------------------------------------------------
# 4. DAG DEFINITION
# ------------------------------------------------

with DAG(
    dag_id="sqlserver_to_gcs_csv",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["sqlserver", "gcs", "csv"]
) as dag:

    # ------------------------------------------------
    # 5. SQL SERVER → GCS TASK
    # ------------------------------------------------

    extract_sqlserver_to_gcs = MsSqlToGCSOperator(
        task_id="extract_sqlserver_to_gcs",

        # Airflow connection id for SQL Server
        mssql_conn_id="sqlserver_conn",

        # SQL query to extract data
        sql="""
            SELECT *
            FROM dbo.customer
            --WHERE updated_at >= '{{ ds }}' --increment
        """,

        # GCS bucket name
        bucket="my-gcs-bucket",

        # File path inside bucket
        filename="sqlserver/customer/{{ ds }}/customer_data.csv",

        # Export format
        export_format="csv",

        # Field delimiter for CSV
        field_delimiter=",",

        # Whether to include header row
        include_header=True,

        # GCP connection
        gcp_conn_id="google_cloud_default"
    )
