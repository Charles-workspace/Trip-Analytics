from __future__ import annotations

from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel, field_validator


class Resultset(BaseModel):
    offset: int
    count: int
    limit: int


class Metadata(BaseModel):
    resultset: Resultset


class WeatherRecord(BaseModel):
    date: str      
    datatype: str
    station: str
    attributes: Optional[str] = None
    value: float

    @field_validator("date", mode="before")
    @classmethod
    def normalize_date(cls, v):
        if isinstance(v, str) and "T" in v:
            return v.split("T")[0]
        if isinstance(v, datetime):
            return v.date().isoformat()
        return v

    @field_validator("value", mode="before")
    @classmethod
    def cast_value(cls, v):
        return float(v) if v is not None else None


class WeatherAPIResponse(BaseModel):
    metadata: Metadata
    results: List[WeatherRecord]