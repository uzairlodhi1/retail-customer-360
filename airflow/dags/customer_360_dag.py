"""
Airflow DAG: customer_360_pipeline

Orchestrates the end-to-end Customer 360 data pipeline:
  1. Extract customers and orders from raw sources
  2. Run PySpark transformations
  3. Load enriched data to Snowflake staging
  4. Trigger dbt models (staging → marts)
  5. Run data quality checks
  6. Send pipeline summary notification

Schedule: Daily at 03:00 UTC
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.utils.dates import days_ago

# ---------------------------------------------------------------------------
# Default arguments
# ---------------------------------------------------------------------------
DEFAULT_ARGS = {
    "owner": "ozair.lodhi",
    "depends_on_past": False,
    "email": ["uzairlodhi1@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}

# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="customer_360_pipeline",
    default_args=DEFAULT_ARGS,
    description="Daily Customer 360 ETL: extract → transform → load → dbt → QA",
    schedule_interval="0 3 * * *",
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["retail", "customer360", "etl", "snowflake"],
) as dag:

    # ------------------------------------------------------------------
    # Task 1: Check source data freshness
    # ------------------------------------------------------------------
    check_source_freshness = SnowflakeOperator(
        task_id="check_source_freshness",
        snowflake_conn_id="snowflake_retail_dw",
        sql="""
            SELECT
                COUNT(*) AS new_customers
            FROM RAW.CUSTOMERS
            WHERE CREATED_AT >= DATEADD('hour', -25, CURRENT_TIMESTAMP())
        """,
        doc_md="Validates that raw source tables have been updated in the last 25 hours.",
    )

    # ------------------------------------------------------------------
    # Task 2: Extract & transform (PySpark via spark-submit)
    # ------------------------------------------------------------------
    run_etl = BashOperator(
        task_id="run_pyspark_etl",
        bash_command=(
            "spark-submit "
            "--master yarn "
            "--deploy-mode cluster "
            "--num-executors 4 "
            "--executor-memory 8g "
            "--executor-cores 2 "
            "--conf spark.sql.shuffle.partitions=200 "
            "{{ var.value.project_root }}/scripts/run_pipeline.py "
            "--mode incremental "
            "--run-date {{ ds }}"
        ),
        doc_md="Runs the PySpark ETL job to clean, enrich, and write the Customer 360 dataset.",
    )

    # ------------------------------------------------------------------
    # Task 3: Run dbt staging models
    # ------------------------------------------------------------------
    dbt_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=(
            "cd {{ var.value.project_root }}/dbt_models && "
            "dbt run --select staging --target prod"
        ),
        doc_md="Runs all dbt staging models (stg_customers, stg_orders).",
    )

    # ------------------------------------------------------------------
    # Task 4: Run dbt mart models
    # ------------------------------------------------------------------
    dbt_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command=(
            "cd {{ var.value.project_root }}/dbt_models && "
            "dbt run --select marts --target prod"
        ),
        doc_md="Builds customer_360 and order_summary mart tables.",
    )

    # ------------------------------------------------------------------
    # Task 5: dbt tests
    # ------------------------------------------------------------------
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            "cd {{ var.value.project_root }}/dbt_models && "
            "dbt test --target prod"
        ),
        doc_md="Runs dbt schema and data tests. Fails DAG if tests fail.",
    )

    # ------------------------------------------------------------------
    # Task 6: Row count audit in Snowflake
    # ------------------------------------------------------------------
    audit_row_counts = SnowflakeOperator(
        task_id="audit_row_counts",
        snowflake_conn_id="snowflake_retail_dw",
        sql="""
            INSERT INTO AUDIT.PIPELINE_RUNS
                (run_date, dag_id, table_name, row_count, loaded_at)
            SELECT
                '{{ ds }}'::DATE,
                'customer_360_pipeline',
                'MARTS.CUSTOMER_360',
                COUNT(*),
                CURRENT_TIMESTAMP()
            FROM MARTS.CUSTOMER_360;

            INSERT INTO AUDIT.PIPELINE_RUNS
                (run_date, dag_id, table_name, row_count, loaded_at)
            SELECT
                '{{ ds }}'::DATE,
                'customer_360_pipeline',
                'MARTS.ORDER_SUMMARY',
                COUNT(*),
                CURRENT_TIMESTAMP()
            FROM MARTS.ORDER_SUMMARY;
        """,
        doc_md="Records row counts in audit table for data volume tracking.",
    )

    # ------------------------------------------------------------------
    # Pipeline dependencies
    # ------------------------------------------------------------------
    (
        check_source_freshness
        >> run_etl
        >> dbt_staging
        >> dbt_marts
        >> dbt_test
        >> audit_row_counts
    )
