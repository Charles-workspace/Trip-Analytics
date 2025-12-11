# src/trip_pipeline/sproc/load_trip_sproc.py

from snowflake.snowpark import Session
from trip_pipeline.ingestion.load_trip_from_stage import load_trip_from_stage


def run(session: Session) -> str:
    """
    Stored Procedure: Loads taxi trip parquet + taxi zone lookup CSV
    from the Snowflake landing stage into Snowflake landing tables.

    This sproc performs ONLY the ingestion step (no transforms).
    """
    try:
        load_trip_from_stage(session)
        return "SP_LOAD_TRIP_FROM_STAGE: Trip data successfully loaded."
    except Exception as e:
        return f"SP_LOAD_TRIP_FROM_STAGE: Failed with error: {e}"