
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, lit, concat_ws
from src.transform.trip_data_transformer import trip_records_transformer
from src.transform.weather_data_transformer import pivot_weather_table
from src.dq.dq_check import DQCheck
from src.configs.data_objects import config
from src.configs.connection_config import connection_parameters
from dotenv import load_dotenv


def main():

    load_dotenv("/Users/charlessanthakumar/Documents/Trip-Analytics/set_sf_env.sh", override=True)
    session = Session.builder.configs(vars(connection_parameters)).create()

    df_trip_orig = session.table(config.raw_trip_data)
    df_weather = session.table(config.raw_weather_data)
    
    weather_df = pivot_weather_table() 
    trip_df = trip_records_transformer()

    dq = DQCheck(session, config.trip_key_cols, config.dq_table_name)

    # Null Check
    null_count, df_after_nulls = dq.null_check(df_trip_orig, config.dq_table_name)
    if null_count == 0:
        print("Proceeding to duplicate check")
    else:
        print("Nulls found. Review before proceeding.")

    df_after_dupes = dq.duplicate_check(df_after_nulls,config.dq_table_name)

    # Junk Check
    df_junk_ts,df_clean_ts = dq.junk_value_check(df_after_dupes, config.trip_ts_columns, "timestamp_type")
    df_junk_int,df_clean_int = dq.junk_value_check(df_clean_ts, config.trip_int_columns, "integer_type")

    df_junk_int.write.mode("overwrite").save_as_table(config.invalid_trip_data)
    df_junk_ts.write.mode("append").save_as_table(config.invalid_trip_data)
    df_clean_int.write.mode("overwrite").save_as_table(config.valid_trip_data)
    print (f"{df_clean_int.count()} Total valid trip records written into {config.valid_trip_data} table")

    ### Weather Data DQ Check ###
    dq = DQCheck(session, config.weather_key_cols, config.dq_table_name)

    # Null Check
    null_count, df_after_nulls = dq.null_check(df_weather, config.weather_null)
    if null_count == 0:
        print("Proceeding to duplicate check")
    else:
        print("Nulls found. Review before proceeding.")

    df_after_dupes = dq.duplicate_check(df_after_nulls, config.weather_null)

    # Junk Check
    df_junk_ts,df_clean_ts = dq.weather_junk_val_check(df_after_dupes, config.weather_ts_columns, "timestamp_type", config.weather_data_types )
    df_junk_int,df_clean_int = dq.weather_junk_val_check(df_clean_ts, config.weather_data_columns, "datatype_check", config.weather_data_types)

    df_junk_int.write.mode("overwrite").save_as_table(config.invalid_weather_data)
    df_clean_int.write.mode("overwrite").save_as_table(config.valid_weather_data)
    print (f"{df_clean_int.count()} Total valid weather data records written into {config.valid_weather_data} table")
    
    

    z_lookup = session.table(config.zone_lookup)
    z_pickup = z_lookup.select(
        col("LOCATIONID").alias("Pu_location_id"),
        col("BOROUGH").alias("pu_borough"),
        col("zone").alias("pu_zone"))

    z_drop = z_lookup.select(
        col("LOCATIONID").alias("do_location_id"),
        col("BOROUGH").alias("do_borough"),
        col("zone").alias("do_zone"))


    join_df = trip_df.join(z_pickup,
                        trip_df['"PULocationID"']==z_pickup["Pu_location_id"]
                        ).join(z_drop,
                               trip_df['"DOLocationID"']==z_drop["do_location_id"]
                               ).join(weather_df,
                                      trip_df['"RideDate"'] == weather_df['"ODate"'])

    final_df= join_df.with_column('"PickupAddress"',
                                  concat_ws(lit(','),join_df["pu_zone"],join_df["pu_borough"])
                                  ).with_column('"DropAddress"',
                                                concat_ws(lit(','),join_df["do_zone"],join_df["do_borough"]))


    final_df=final_df.select('"VendorID"','"PickupAddress"','"DropAddress"','"PickupTime"','"DropoffTime"','"TripDistance"','"TotalAmount"','"Tmin"','"Tmax"','"Prcp"','"Snow"','"Snwd"','"Awnd"','"Wsf2"','"Wdf2"','"Wsf5"','"Wdf5"')
    final_df.write.mode("overwrite").saveAsTable(config.final_table)

if __name__ == "__main__":
    main()