# src/trip_pipeline/sproc/run_trip_pipeline_sproc.py

from snowflake.snowpark import Session
from trip_pipeline.app import main as pipeline_main


def run(session: Session) -> str:
    """
    Stored Procedure: Executes the full Trip Analytics ETL pipeline.

    Assumes:
      - Weather data has already been loaded into landing table
      - Trip data has already been loaded into landing table

    This sproc runs the transformations, DQ checks, joins, pivots,
    and writes the final curated analytics table.
    """
    try:
        pipeline_main(session)
        return (f"SP_RUN_TRIP_PIPELINE: Pipeline executed successfully ")
    except Exception as e:
        return f"SP_RUN_TRIP_PIPELINE failed: {e}"