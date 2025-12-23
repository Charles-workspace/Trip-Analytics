from dataclasses import dataclass
from datetime import date

@dataclass(frozen=True)
class Tableconfig:
    """
    This class holds the table names, key columns parameterized which are used throughout 
    the pipeline.
    """
    trip_key_cols: list
    weather_key_cols: list
    weather_data_types: list
    trip_ts_columns: list
    trip_int_columns: list
    weather_ts_columns: list
    weather_data_columns: list

    dq_table_name: str
    invalid_trip_data: str
    valid_trip_data: str
    trip_duplicates: str
    raw_trip_data: str
    weather_null: str
    weather_duplicates: str
    valid_weather_data: str
    invalid_weather_data: str
    raw_weather_data: str
    pivoted_weather_table: str
    zone_lookup: str
    final_table: str
    weather_station_id: str
    weather_stage_name: str
    weather_start_date: date
    weather_end_date: date

config = Tableconfig(
trip_key_cols = ['"VendorID"', '"tpep_pickup_datetime"',
                 '"tpep_dropoff_datetime"','"PULocationID"','"DOLocationID"'],
weather_data_types = ["AWND", "WT01", "WSF5", "WSF2", "WDF5", "WDF2", "TMIN",
                      "TMAX", "SNWD", "PRCP", "WT08", "SNOW", "WT03", "WT02"],

trip_ts_columns = ['"tpep_pickup_datetime"', '"tpep_dropoff_datetime"'],
trip_int_columns = ['"VendorID"', '"PULocationID"', '"DOLocationID"'],


weather_ts_columns = ['"date"'],
weather_data_columns = ['"datatype"'],
weather_key_cols = ['"date"', '"datatype"'],


dq_table_name="INBOUND_INTEGRATION.DQ_TRIP.TRIP_DATA_NULL_RECORDS",
invalid_trip_data = "INBOUND_INTEGRATION.DQ_TRIP.INVALID_TRIP_DATA",
valid_trip_data="INBOUND_INTEGRATION.SDS_TRIP.TRIP_DATA_VALIDATED",
trip_duplicates = "INBOUND_INTEGRATION.DQ_TRIP.TRIP_DATA_DUPLICATES",
raw_trip_data = "INBOUND_INTEGRATION.LANDING_TRIP.YELLOW_TRIP_RECORDS",

weather_null = "INBOUND_INTEGRATION.DQ_WEATHER.WEATHER_NULL_RECORDS",
weather_duplicates = "INBOUND_INTEGRATION.DQ_WEATHER.WEATHER_DATA_DUPLICATES",
valid_weather_data = "INBOUND_INTEGRATION.SDS_WEATHER.WEATHER_DATA_VALIDATED",
invalid_weather_data = "INBOUND_INTEGRATION.DQ_WEATHER.INVALID_WEATHER_DATA",
raw_weather_data="INBOUND_INTEGRATION.LANDING_WEATHER.NYC_WEATHER",

pivoted_weather_table = "INBOUND_INTEGRATION.SDS_WEATHER.WEATHER_DATA_PIVOTED",

zone_lookup = "INBOUND_INTEGRATION.LANDING_TRIP.TAXI_ZONE_LOOKUP",
final_table = "OUTBOUND_INTEGRATION.TRIP_ANALYTICS.TRIP_ANALYTICS",

weather_station_id="GHCND:USW00094728",
weather_stage_name="INBOUND_INTEGRATION.LANDING_WEATHER.LANDING_WEATHER_STAGE",
weather_start_date=date(2025, 5, 1),
weather_end_date=date(2025, 5, 3)
)
