from trip_pipeline.utils.io_utils import copy_into_table


def load_weather_from_stage(session):

    stage = "INBOUND_INTEGRATION.LANDING_WEATHER.LANDING_WEATHER_STAGE"
    table = "INBOUND_INTEGRATION.LANDING_WEATHER.NYC_WEATHER"

    copy_into_table(
        session=session,
        table_name=table,
        stage_name=stage,
        file_format_type="CSV",
        file_format_options={
            "FIELD_OPTIONALLY_ENCLOSED_BY": "'\"'",
            "PARSE_HEADER": "TRUE"
        },

    )

    print("Weather landing table loaded from stage.")

