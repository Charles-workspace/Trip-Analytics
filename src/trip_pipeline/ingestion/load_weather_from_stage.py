from trip_pipeline.utils.io_utils import copy_into_table
from trip_pipeline.configs.data_objects import config
from utils.logger import get_logger

logger = get_logger(__name__)

def load_weather_from_stage(session):

    copy_into_table(
        session=session,
        table_name=config.raw_weather_data,
        stage_name=config.weather_stage_name,
        file_format_type="CSV",
        file_format_options={
            "FIELD_OPTIONALLY_ENCLOSED_BY": "'\"'",
            "PARSE_HEADER": "TRUE"
        },

    )

    logger.info("Weather data loaded from stage to table %s", config.raw_weather_data)

