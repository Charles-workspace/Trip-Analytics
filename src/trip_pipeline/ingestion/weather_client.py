from __future__ import annotations

from datetime import date
from typing import List, Optional, Dict, Any

from trip_pipeline.ingestion.base_client import BaseAPIClient
from trip_pipeline.schemas.weather import WeatherAPIResponse, WeatherRecord


class NOAAWeatherClient(BaseAPIClient):
    """
    Client for NOAA CDO `data` endpoint.

    Docs: https://www.ncdc.noaa.gov/cdo-web/webservices/v2
    """

    def fetch_daily_weather(
        self,
        station_id: str,
        start_date: date,
        end_date: date,
        dataset_id: str = "GHCND",
        datatype_ids: Optional[list[str]] = None,
        units: str = "metric",
        limit: int = 1000,
    ) -> List[WeatherRecord]:
        """
        Fetches daily weather observations for a station and date range.

        Returns:
            List[WeatherRecord]: Pydantic-validated list of records.
        """
        params: Dict[str, Any] = {
            "datasetid": dataset_id,
            "stationid": station_id,
            "startdate": start_date.isoformat(),
            "enddate": end_date.isoformat(),
            "units": units,
            "limit": limit,
        }

        if datatype_ids:
            # NOAA allows multiple datatypeid values as repeated query params.
            params["datatypeid"] = datatype_ids

        raw = self.get("data", params=params)
        api_response = WeatherAPIResponse.model_validate(raw)  
        return api_response.results