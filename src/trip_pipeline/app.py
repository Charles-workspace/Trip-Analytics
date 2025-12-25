
from snowflake.snowpark.functions import col, lit, concat_ws
from snowflake.snowpark.exceptions import SnowparkSQLException
from trip_pipeline.transform.trip_data_transformer import trip_records_transformer
from trip_pipeline.transform.weather_data_transformer import pivot_weather_table
from trip_pipeline.dq.dq_check import DQCheck
from trip_pipeline.configs.data_objects import config
from trip_pipeline.utils.io_utils import copy_into_table
from trip_pipeline.utils.logger import get_logger

def main(session):
    logger = get_logger(__name__)

    try:

        if session is None:
            logger.error("Snowpark session is not provided. Exiting the application.")
            raise ValueError("Session must be provided.")

        logger.info("Starting Trip Data Pipeline")
        df_trip_orig = session.table(config.raw_trip_data)

        logger.info("Starting Trip data DQ checks")
        dq = DQCheck(session, config.trip_key_cols)

        # Trip data null check
        df_after_nulls = dq.null_check(df_trip_orig, config.trip_null)

        # Trip data duplicate check
        df_after_dupes = dq.duplicate_check(df_after_nulls,config.trip_duplicates)

        # Trip data invalid junk data check
        df_junk_ts,df_clean_ts = dq.validate_trip_data(df_after_dupes, config.trip_ts_columns,
                                                    "timestamp_type")
        df_junk_int,df_clean_int = dq.validate_trip_data(df_clean_ts, config.trip_int_columns,
                                                    "integer_type")
        
        datatype_invalids = df_junk_ts.union(df_junk_int)
        invalids_count = datatype_invalids.count()
        clean_count = df_clean_int.count()

        if invalids_count > 0:
            datatype_invalids.write.mode("overwrite").save_as_table(config.invalid_trip_data)
            logger.warning("%d Invalid trip data records with junk values found", invalids_count)
        else:
            df_clean_int.write.mode("overwrite").save_as_table(config.valid_trip_data)
            logger.info("No Invalid trip data records with junk values found."
            " %d Valid trip data records written into %s table", 
                    clean_count,config.valid_trip_data )

        logger.info("Copying Weather data from stage %s into %s",
                config.weather_stage_name,
                config.raw_weather_data)
        
        copy_into_table(
            session=session,
            table_name=config.raw_weather_data,
            stage_name=config.weather_stage_name,
            file_format_type="CSV",
            file_format_options={"PARSE_HEADER": "TRUE",
                "FIELD_OPTIONALLY_ENCLOSED_BY": "'\"'",
                },)

        df_weather = session.table(config.raw_weather_data)

        logger.info("Starting Weather DQ checks")
        dq = DQCheck(session, config.weather_key_cols)

        df_after_nulls = dq.null_check(df_weather, config.weather_null)
        
        df_after_dupes = dq.duplicate_check(df_after_nulls, config.weather_duplicates)

        df_junk_ts,df_clean_ts = dq.validate_weather_data(df_after_dupes,
                                                        config.weather_ts_columns, "timestamp_type",
                                                        config.weather_data_types)
        df_junk_int,df_clean_int = dq.validate_weather_data(df_clean_ts,
                                                            config.weather_data_columns,
                                                            "datatype_check",
                                                            config.weather_data_types)
        
        datatype_invalids = df_junk_ts.union(df_junk_int)
        invalids_count = datatype_invalids.count()
        clean_count = df_clean_int.count()

        if invalids_count > 0:
            datatype_invalids.write.mode("overwrite").save_as_table(config.invalid_weather_data)
            logger.warning("%d Invalid Weather data records with junk values found", invalids_count)
        else:
            df_clean_int.write.mode("overwrite").save_as_table(config.valid_weather_data)
            logger.info("No Invalid Weather data records with junk values found."
            "%d Valid Weather data records written into %s table", 
                    clean_count,config.valid_weather_data )

        weather_df = pivot_weather_table(session,config.valid_weather_data)
        weather_df.write.mode("overwrite").save_as_table(config.pivoted_weather_table)
        logger.info("Pivoted Weather data written into %s table", 
                    config.pivoted_weather_table)

        z_lookup = session.table(config.zone_lookup)
        z_pickup = z_lookup.select(
            col("LOCATIONID").alias("pu_location_id"),
            col("BOROUGH").alias("pu_borough"),
            col("zone").alias("pu_zone"))

        z_drop = z_lookup.select(
            col("LOCATIONID").alias("do_location_id"),
            col("BOROUGH").alias("do_borough"),
            col("zone").alias("do_zone"))

        trip_df = trip_records_transformer(session, config.valid_trip_data)

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
        logger.info("Trip Data Pipeline completed successfully")

    except SnowparkSQLException as e:
        logger.error("SQL execution failed: %s", e)
        raise

    except Exception:
        logger.exception("Job failed due to unexpected error")
        raise