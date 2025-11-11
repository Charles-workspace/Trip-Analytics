resource "snowflake_stage" "landing_trip_stage" {
  name     = "LANDING_TRIP_STAGE"
  database = snowflake_database.inbound_integration.name
  schema   = snowflake_schema.landing_trip.name

  depends_on = [
    snowflake_schema.landing_trip
  ]
}

resource "snowflake_stage" "landing_weather_stage" {
  name     = "LANDING_WEATHER_STAGE"
  database = snowflake_database.inbound_integration.name
  schema   = snowflake_schema.landing_weather.name

  depends_on = [
    snowflake_schema.landing_weather
  ]
}

resource "snowflake_stage" "trip_analytics_container_stage" {
  name     = "TRIP_ANALYTICS_CONTAINER_STAGE"
  database = snowflake_database.ops.name
  schema   = snowflake_schema.spcs_deployment.name

  depends_on = [
    snowflake_schema.spcs_deployment
  ]
}

resource "snowflake_stage" "proc_stage" {
  name     = "PROC_STAGE"
  database = snowflake_database.ops.name
  schema   = snowflake_schema.proc.name

  depends_on = [
    snowflake_schema.proc
  ]
  comment = "Stage to store wheel package for stored procedure"
}