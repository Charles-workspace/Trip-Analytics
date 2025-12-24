
from snowflake.snowpark.functions import to_timestamp,to_date,col
from utils.logger import get_logger

logger = get_logger(__name__)

def normalize_trip_columns(df):
    rename_map = {}
    for c in df.columns:
        clean = c.replace('"', '').lower()
        rename_map[c] = clean

    for old, new in rename_map.items():
        df = df.with_column_renamed(old, new)

    return df

def trip_records_transformer(session, valid_trip_data):
    
    logger.info("Trip transformation started")

    df = session.table(valid_trip_data)
    
    transformed_df=(
        df.with_column("pickup_time",to_timestamp(col("tpep_pickup_datetime")))
        .with_column("dropoff_time",to_timestamp(col("tpep_dropoff_datetime")))
        .with_column("ride_date", to_date(col("tpep_pickup_datetime")))
        .with_column_renamed("VendorID","vendor_id" )
        .with_column_renamed("PULocationID", "pu_location_id")
        .with_column_renamed("DOLocationID", "do_location_id")
        .with_column_renamed("Airport_fee", "airport_fee")
        )
    
    final_df = transformed_df.select(
        "vendor_id","ride_date","pickup_time","dropoff_time","trip_distance",
        "pu_location_id","do_location_id","fare_amount","tip_amount","tolls_amount","total_amount") 
    
    final_df.count()
    logger.info("Trip transformation completed. %d records transformed.", final_df.count())
    return final_df