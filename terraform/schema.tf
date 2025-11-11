resource "snowflake_schema" "landing_trip" {
  name     = "LANDING_TRIP"
  database = snowflake_database.inbound_integration.name
  depends_on = [
    snowflake_database.inbound_integration
  ]
  comment  = "Landing zone for NYC trip data"
}

resource "snowflake_schema" "landing_weather" {
  name     = "LANDING_WEATHER"
  database = snowflake_database.inbound_integration.name
  depends_on = [
    snowflake_database.inbound_integration
  ]
  comment  = "Landing zone for weather data"
}

resource "snowflake_schema" "staging_trip" {
  name     = "SDS_TRIP"
  database = snowflake_database.inbound_integration.name
  depends_on = [
    snowflake_database.inbound_integration
  ]
  comment  = "Staging zone for cleaned NYC trip data"
}

resource "snowflake_schema" "staging_weather" {
  name     = "SDS_WEATHER"
  database = snowflake_database.inbound_integration.name
  depends_on = [
    snowflake_database.inbound_integration
  ]
  comment  = "Staging zone for cleaned weather data"
}

resource "snowflake_schema" "dq_check_trip" {
  name     = "DQ_TRIP"
  database = snowflake_database.inbound_integration.name
  depends_on = [
    snowflake_database.inbound_integration
  ]
  comment  = "Schema to hold DQ check tables for Trip Data"
}

resource "snowflake_schema" "dq_check_weather" {
  name     = "DQ_WEATHER"
  database = snowflake_database.inbound_integration.name
  depends_on = [
    snowflake_database.inbound_integration
  ]
  comment  = "Schema to hold DQ check tables for Weather Data"
}

resource "snowflake_schema" "trip_analytics" {
  name     = "TRIP_ANALYTICS"
  database = snowflake_database.outbound_integration.name
  depends_on = [
    snowflake_database.outbound_integration
  ]
  comment  = "Schema for processed final data"
}

resource "snowflake_schema" "spcs_deployment" {
  name     = "SPCS_DEPLOYMENT"
  database = snowflake_database.ops.name
  depends_on = [
    snowflake_database.ops
  ]
  comment  = "Schema for storing docker images"
}

resource "snowflake_schema" "proc" {
  name     = "PROC"
  database = snowflake_database.ops.name
  depends_on = [
    snowflake_database.ops
  ]
  comment  = "Schema for stored procedure"
}