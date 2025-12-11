# src/trip_pipeline/sproc/load_weather_sproc.py

from snowflake.snowpark import Session
from trip_pipeline.ingestion.load_weather_from_stage import load_weather_from_stage


def run(session: Session, start_date: str, end_date: str) -> str:
    """
    Stored Procedure: Loads weather CSV data from Snowflake landing stage
    into the landing weather table.
    """
    try:
        load_weather_from_stage(session, start_date, end_date)
        return (f"SP_LOAD_WEATHER_FROM_STAGE: Weather data loaded successfully "
                f"for {start_date} to {end_date}.")
    except Exception as e:
        return f"SP_LOAD_WEATHER_FROM_STAGE failed: {e}"