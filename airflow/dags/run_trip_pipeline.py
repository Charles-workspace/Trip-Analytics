from datetime import datetime,timedelta
from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datasets import weather_ready, trip_ready
from airflow.datasets import Dataset
from airflow.timetables.trigger import CronTriggerTimetable
from airflow.timetables.datasets import DatasetOrTimeSchedule


with DAG(
    dag_id="run_trip_pipeline",
    start_date=datetime(2025, 12, 22),
    schedule=DatasetOrTimeSchedule(
        datasets=[weather_ready, trip_ready],
        timetable=CronTriggerTimetable("30 21 * * *", timezone="EET")
),
    catchup=False
) as dag:

    run_pipeline = SnowflakeOperator(
        task_id="sp_run_trip_pipeline",
        sql="CALL OPS.PROC.SP_RUN_TRIP_PIPELINE();",
        snowflake_conn_id="snowflake_default",
        retries=1,
        retry_delay=timedelta(minutes=10),
    )

    run_pipeline