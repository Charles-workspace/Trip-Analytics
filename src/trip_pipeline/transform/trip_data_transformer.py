from snowflake.snowpark import Session
from snowflake.snowpark.functions import to_timestamp,to_date
from src.configs.connection_config import connection_parameters

valid_trip_data="INBOUND_INTEGRATION.SDS_TRIP.TRIP_DATA_VALIDATED"

def trip_records_transformer():
    session = Session.builder.configs(vars(connection_parameters)).create()
    df = session.table(valid_trip_data)
    
    transformed_df=(df.with_column('"PickupTime"',to_timestamp('"tpep_pickup_datetime"'))
        .with_column('"DropoffTime"',to_timestamp('"tpep_dropoff_datetime"'))
        .with_column('"RideDate"', to_date('"tpep_pickup_datetime"'))
        .with_column_renamed('"passenger_count"', '"PassengerCount"')
        .with_column_renamed('"trip_distance"', '"TripDistance"')
        .with_column_renamed('"fare_amount"', '"FareAmount"')
        .with_column_renamed('"tip_amount"', '"TipAmount"')
        .with_column_renamed('"tolls_amount"', '"TollsAmount"')
        .with_column_renamed('"total_amount"', '"TotalAmount"')
        .with_column_renamed('"Airport_fee"', '"AirportFee"')
        .with_column_renamed('"cbd_congestion_fee"', '"CbdCongestionFee"')
        )
    
    return transformed_df.select('"VendorID"','"RideDate"','"PickupTime"','"DropoffTime"','"TripDistance"','"PULocationID"','"DOLocationID"','"FareAmount"','"TipAmount"','"TollsAmount"','"TotalAmount"') 