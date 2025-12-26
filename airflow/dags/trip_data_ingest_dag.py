import sys
sys.path.append("/opt/airflow/src")

from datetime import datetime
from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datasets import trip_ready

with DAG(
    dag_id="load_trip_data",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False
) as dag:

    sp_load_trip = SnowflakeOperator(
        task_id="sp_load_trip_from_stage",
        sql="call OPS.PROC.SP_LOAD_TRIP_FROM_STAGE();",
        snowflake_conn_id="snowflake_default",
        outlets=trip_ready
    )

    sp_load_trip  # Standalone DAG