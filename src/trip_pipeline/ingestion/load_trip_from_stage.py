from trip_pipeline.utils.io_utils import  copy_into_table
from trip_pipeline.configs.data_objects import config
from trip_pipeline.utils.logger import get_logger

logger = get_logger(__name__)

def load_trip_from_stage(session):
    """
    Load trip data from stage to raw trip table.
    Args:
        session: Snowflake Snowpark session object from the S-proc wrapper
    """

    # Trip parquet
    copy_into_table(
        session=session,
        table_name=config.raw_trip_data,
        stage_name=config.trip_stage_name,
        file_format_type="PARQUET",
        pattern=".*yellow_tripdata_.*\\.parquet.*",
    )
    logger.info("Trip data loaded from stage to landing table.")

    # Taxi zone lookup CSV
    copy_into_table(
        session=session,
        table_name=config.zone_lookup,
        stage_name=config.trip_stage_name,
        file_format_type="CSV",
        file_format_options={
            "FIELD_OPTIONALLY_ENCLOSED_BY": "'\"'",
            "PARSE_HEADER": "TRUE"
        },
        pattern=".*taxi_zone_lookup.csv",
        match_by_column_name="CASE_INSENSITIVE",
        force=True
    )
    
    logger.info("Trip data and zone lookup data loaded from stage to landing tables.")
