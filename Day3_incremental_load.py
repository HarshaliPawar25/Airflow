
# Import DAG class to define an Airflow DAG
from airflow import DAG

# Operator to run BigQuery SQL jobs
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

# Import datetime utilities
from datetime import datetime, timedelta


# -------------------------
# DEFAULT DAG ARGUMENTS
# -------------------------
default_args = {
    "owner": "data-engineering",             # Team responsible for DAG
    "depends_on_past": False,                # Each run is independent
    "email_on_failure": True,                # Notify on failure
    "retries": 2,                            # Retry twice if task fails
    "retry_delay": timedelta(minutes=5),     # Wait 5 minutes between retries
}


# -------------------------
# DAG DEFINITION
# -------------------------
with DAG(
    dag_id="incremental_customer_load",      # Unique DAG name
    description="Incremental load with watermark and backfill support",
    start_date=datetime(2024, 1, 1),          # DAG start date
    schedule_interval="0 3 * * *",            # Run daily at 3 AM
    catchup=False,                            # Disable automatic backfill
    default_args=default_args,
    tags=["incremental", "watermark", "batch"],
) as dag:


    # ----------------------------------------------------
    # TASK 1: READ LAST WATERMARK VALUE
    # ----------------------------------------------------
    # Watermark table keeps track of last successful load
    get_last_watermark = BigQueryInsertJobOperator(
        task_id="get_last_watermark",

        configuration={
            "query": {
                # This query fetches the last processed timestamp
                "query": """
                CREATE OR REPLACE TABLE dataset.temp_watermark AS
                SELECT
                    COALESCE(MAX(last_processed_ts), TIMESTAMP('1970-01-01')) 
                    AS last_processed_ts
                FROM dataset.watermark_table
                WHERE table_name = 'customer_curated'
                """,
                "useLegacySql": False,
            }
        },

        location="US",
    )


    # ----------------------------------------------------
    # TASK 2: INCREMENTAL / DELTA LOAD
    # ----------------------------------------------------
    incremental_customer_load = BigQueryInsertJobOperator(
        task_id="incremental_customer_load",

        configuration={
            "query": {
                # Incremental MERGE logic
                "query": """
                MERGE dataset.customer_curated T
                USING (
                    SELECT *
                    FROM dataset.customer_source
                    WHERE updated_at >
                          (SELECT last_processed_ts FROM dataset.temp_watermark)
                ) S
                ON T.customer_id = S.customer_id

                -- Update existing customer records
                WHEN MATCHED THEN
                  UPDATE SET
                    name = S.name,
                    email = S.email,
                    updated_at = S.updated_at

                -- Insert new customer records
                WHEN NOT MATCHED THEN
                  INSERT (customer_id, name, email, updated_at)
                  VALUES (S.customer_id, S.name, S.email, S.updated_at)
                """,

                "useLegacySql": False,
            }
        },

        location="US",
    )


    # ----------------------------------------------------
    # TASK 3: UPDATE WATERMARK AFTER SUCCESSFUL LOAD
    # ----------------------------------------------------
    update_watermark = BigQueryInsertJobOperator(
        task_id="update_watermark",

        configuration={
            "query": {
                # Update watermark table with latest timestamp
                "query": """
                INSERT INTO dataset.watermark_table (table_name, last_processed_ts)
                SELECT
                    'customer_curated' AS table_name,
                    MAX(updated_at) AS last_processed_ts
                FROM dataset.customer_curated
                """,
                "useLegacySql": False,
            }
        },

        location="US",
    )


    # ----------------------------------------------------
    # TASK DEPENDENCIES
    # ----------------------------------------------------
    # Watermark must be read first
    # Incremental load runs next
    # Watermark updated only after success
    get_last_watermark >> incremental_customer_load >> update_watermark




🔁 Backfill strategy (VERY IMPORTANT)
Option 1: Manual backfill
Change watermark manually:
UPDATE dataset.watermark_table
SET last_processed_ts = TIMESTAMP('2024-01-01')
WHERE table_name = 'customer_curated';

Option 2: Parameterized backfill (advanced)
Pass start_date via Airflow variables
Override watermark logic
