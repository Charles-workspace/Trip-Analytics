from snowflake.snowpark import Session
from trip_pipeline.app import main as trip_etl_main

def run_trip_pipeline(session: Session, start_date: str, end_date: str):
    """
    Stored proc wrapper that calls the main Snowpark trip ETL pipeline.
    Weather data must already be loaded into landing table in Snowflake.
    """

    session.sql(f"ALTER SESSION SET TIMEZONE = 'UTC';").collect()

    # Call your actual ETL entry point
    trip_etl_main(start_date, end_date)


    return f"Trip ETL executed for {start_date} to {end_date}"