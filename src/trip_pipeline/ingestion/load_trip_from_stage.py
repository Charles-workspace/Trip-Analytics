from trip_pipeline.utils.io_utils import  copy_into_table


def load_trip_from_stage(session):

    stage = "INBOUND_INTEGRATION.LANDING_TRIP.LANDING_TRIP_STAGE"
    trip_table = "INBOUND_INTEGRATION.LANDING_TRIP.YELLOW_TRIP_RECORDS"
    zone_table = "INBOUND_INTEGRATION.LANDING_TRIP.TAXI_ZONE_LOOKUP"

    # Trip parquet
    copy_into_table(
        session=session,
        table_name=trip_table,
        stage_name=stage,
        file_format_type="PARQUET",
        pattern=".*yellow_tripdata_.*\\.parquet.*",
    )

    # Taxi zone lookup CSV
    copy_into_table(
        session=session,
        table_name=zone_table,
        stage_name=stage,
        file_format_type="CSV",
        file_format_options={
            "FIELD_OPTIONALLY_ENCLOSED_BY": "'\"'",
            "PARSE_HEADER": "TRUE"
        },
        pattern=".*taxi_zone_lookup.\\.csv.*",
    )
    print("Trip tables loaded from stage.")
