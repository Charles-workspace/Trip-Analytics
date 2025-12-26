import sys
sys.path.append("/opt/airflow/src")

from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
import os
from trip_pipeline.ingestion.weather_ingestion_service import fetch_and_stage_weather
from trip_pipeline.configs.data_objects import config
from snowflake.snowpark import Session
from datetime import timedelta
from datasets import weather_ready

def get_session():
    return Session.builder.configs(
        {
            "account": os.getenv("SNOWFLAKE_ACCOUNT"),
            "user": os.getenv("SNOWFLAKE_USER"),
            "password": os.getenv("SNOWFLAKE_PASSWORD")
        }
    ).create()


def fetch_weather():
    session = get_session()
    fetch_and_stage_weather(
        session=session,
        station_id=config.weather_station_id,
        start_date=config.weather_start_date,
        end_date=config.weather_end_date,
        stage_name=config.weather_stage_name,
    )

with DAG(
    dag_id="weather_ingest",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    fetch = PythonOperator(
        task_id="fetch_weather_from_api",
        python_callable=fetch_weather,
    )

    run_weather_sproc = SnowflakeOperator(
    task_id="sp_load_weather",
    sql="CALL OPS.PROC.SP_LOAD_WEATHER_FROM_STAGE();",
    snowflake_conn_id="snowflake_default",
    retries=1,
    retry_delay=timedelta(minutes=1),
    outlets=[weather_ready]
)

    fetch >> run_weather_sproc

