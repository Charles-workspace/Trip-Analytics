
from snowflake.snowpark.functions import col, lit, concat_ws
from trip_pipeline.transform.trip_data_transformer import trip_records_transformer
from trip_pipeline.transform.weather_data_transformer import pivot_weather_table
from trip_pipeline.dq.dq_check import DQCheck
from trip_pipeline.configs.data_objects import config


from trip_pipeline.utils.io_utils import copy_into_table



def main(session):


    df_trip_orig = session.table(config.raw_trip_data)

    dq = DQCheck(session, config.trip_key_cols, config.dq_table_name)

    # Null Check
    null_count, df_after_nulls = dq.null_check(df_trip_orig, config.dq_table_name)
    if null_count == 0:
        print("Proceeding to duplicate check")
    else:
        print("Nulls found. Review before proceeding.")

    df_after_dupes = dq.duplicate_check(df_after_nulls,config.dq_table_name)

    # Junk Check
    df_junk_ts,df_clean_ts = dq.junk_value_check(df_after_dupes, config.trip_ts_columns,
                                                 "timestamp_type")
    df_junk_int,df_clean_int = dq.junk_value_check(df_clean_ts, config.trip_int_columns,
                                                   "integer_type")

    df_junk_int.write.mode("overwrite").save_as_table(config.invalid_trip_data)
    df_junk_ts.write.mode("append").save_as_table(config.invalid_trip_data)
    df_clean_int.write.mode("overwrite").save_as_table(config.valid_trip_data)
    print (
        f"{df_clean_int.count()} Total valid trip records "
        f"written into {config.valid_trip_data} table")


    copy_into_table(
        session=session,
        table_name=config.raw_weather_data,
        stage_name=config.weather_stage_name,
        file_format_type="CSV",
        file_format_options={"PARSE_HEADER": "TRUE",
            "FIELD_OPTIONALLY_ENCLOSED_BY": "'\"'",
            },)

    df_weather = session.table(config.raw_weather_data)

    # Weather Data DQ Check ###
    dq = DQCheck(session, config.weather_key_cols, config.dq_table_name)

    # Null Check
    null_count, df_after_nulls = dq.null_check(df_weather, config.weather_null)
    if null_count == 0:
        print("Proceeding to duplicate check")
    else:
        print("Nulls found. Review before proceeding.")

    df_after_dupes = dq.duplicate_check(df_after_nulls, config.weather_null)

    # Junk Check
    df_junk_ts,df_clean_ts = dq.weather_junk_val_check(df_after_dupes,
                                                       config.weather_ts_columns, "timestamp_type",
                                                       config.weather_data_types)
    df_junk_int,df_clean_int = dq.weather_junk_val_check(df_clean_ts,
                                                         config.weather_data_columns,
                                                         "datatype_check",
                                                         config.weather_data_types)

    df_junk_int.write.mode("overwrite").save_as_table(config.invalid_weather_data)
    df_clean_int.write.mode("overwrite").save_as_table(config.valid_weather_data)
    print (
        f"{df_clean_int.count()} Total valid weather data records "
        f"written into {config.valid_weather_data} table")

    weather_df = pivot_weather_table(session,config.valid_weather_data)
    weather_df.write.mode("overwrite").save_as_table(config.pivoted_weather_table)

    z_lookup = session.table(config.zone_lookup)
    z_pickup = z_lookup.select(
        col("LOCATIONID").alias("pu_location_id"),
        col("BOROUGH").alias("pu_borough"),
        col("zone").alias("pu_zone"))

    z_drop = z_lookup.select(
        col("LOCATIONID").alias("do_location_id"),
        col("BOROUGH").alias("do_borough"),
        col("zone").alias("do_zone"))

    trip_df = trip_records_transformer(session)

    join_df = trip_df.join(z_pickup,
                        trip_df["pu_location_id"]==z_pickup["Pu_location_id"]
                        ).join(z_drop,
                               trip_df["do_location_id"]==z_drop["do_location_id"]
                               ).join(weather_df,
                                      trip_df["ride_date"] == weather_df["o_date"])

    final_df= join_df.with_column("pickup_address",
                                  concat_ws(lit(","),join_df["pu_zone"],join_df["pu_borough"])
                                  ).with_column("drop_address",
                                concat_ws(lit(","),join_df["do_zone"],join_df["do_borough"]))


    final_df=final_df.select("vendor_id","pickup_address","drop_address",
                             "pickup_time","dropoff_time","trip_distance",
                             "total_amount","tmin","tmax","prcp","snow",
                             "snwd","awnd","wsf2","wdf2","wsf5","wdf5")

    final_df.write.mode("overwrite").saveAsTable(config.final_table)
    print("Final table populated successfully")

#if __name__ == "__main__":
#    main()
