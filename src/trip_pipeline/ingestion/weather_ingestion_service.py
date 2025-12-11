import os
import csv
from datetime import date
from io import StringIO

from trip_pipeline.ingestion.weather_client import NOAAWeatherClient
from trip_pipeline.utils.logger import get_logger

logger = get_logger(__name__)

def fetch_and_stage_weather(
    session,
    station_id: str,
    start_date: date,
    end_date: date,
    stage_name: str
):
    """
    Fetches weather data from NOAA API and stages it in Snowflake.
    """

    client = NOAAWeatherClient(
        base_url=os.getenv("NOAA_BASE_URL", "https://www.ncdc.noaa.gov/cdo-web/api/v2"),
        api_key=os.environ["NOAA_TOKEN"]
    )

    records = client.fetch_daily_weather(
        station_id=station_id,
        start_date=start_date,
        end_date=end_date,
    )

    if not records:
        raise ValueError(f"No weather data returned from {start_date} to {end_date}")

    # Convert WeatherRecord list → CSV string
    buffer = StringIO()
    writer = None

    for rec in records:
        row_dict = rec.model_dump()

        if writer is None:
            writer = csv.DictWriter(buffer, fieldnames=row_dict.keys())
            writer.writeheader()

        writer.writerow(row_dict)

    csv_content = buffer.getvalue()

    # Store it into a temp file for PUT
    tmp_path = "/tmp/weather_file.csv"
    with open(tmp_path, "w") as f:
        f.write(csv_content)

    # Upload to Snowflake stage
    session.file.put(
        tmp_path,
        f"@{stage_name}",
        overwrite=True,
        auto_compress=False
    )

    logger.info(f"Uploaded weather CSV to @{stage_name}")
    return tmp_path