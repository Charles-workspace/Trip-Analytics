resource "snowflake_schema" "landing_trip" {
  name     = "LANDING_TRIP"
  database = "INBOUND_INTEGRATION"
  comment  = "Landing zone for NYC trip data"
}

resource "snowflake_schema" "landing_weather" {
  name     = "LANDING_WEATHER"
  database = "INBOUND_INTEGRATION"
  comment  = "Landing zone for weather data"
}

resource "snowflake_schema" "staging_trip" {
  name     = "SDS_TRIP"
  database = "INBOUND_INTEGRATION"
  comment  = "Staging zone for cleaned NYC trip data"
}

resource "snowflake_schema" "staging_weather" {
  name     = "SDS_WEATHER"
  database = "INBOUND_INTEGRATION"
  comment  = "Staging zone for cleaned weather data"
}

resource "snowflake_schema" "dq_check_trip" {
  name     = "DQ_TRIP"
  database = "INBOUND_INTEGRATION"
  comment  = "Schema to hold DQ check tables for Trip Data"
}

resource "snowflake_schema" "dq_check_weather" {
  name     = "DQ_WEATHER"
  database = "INBOUND_INTEGRATION"
  comment  = "Schema to hold DQ check tables for Weather Data"
}

resource "snowflake_schema" "trip_analytics" {
  name     = "TRIP_ANALYTICS"
  database = "OUTBOUND_INTEGRATION"
  comment  = "Schema for processed final data"
}