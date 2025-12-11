from snowflake.snowpark import Session
from trip_pipeline.app import main as trip_etl_main
from trip_pipeline.ingestion.weather_ingestion_service import fetch_weather_and_load


def run_trip_pipeline(session: Session, start_date: str, end_date: str):
    """
    Snowflake Stored Procedure entrypoint.
    """

    # 1. Ingest weather data for the given date range (inside Snowflake)
    fetch_weather_and_load(session, start_date, end_date)

    # 2. Run the ETL pipeline already defined in app.py
    trip_etl_main(session)

    return f"Trip ETL executed for {start_date} to {end_date}"