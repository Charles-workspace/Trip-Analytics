resource "snowflake_stage" "landing_trip_stage" {
  name     = "LANDING_TRIP_STAGE"
  database = "INBOUND_INTEGRATION"
  schema   = "LANDING_TRIP"
}

#resource "snowflake_stage" "landing_weather_stage" {
#  name     = "LANDING_WEATHER_STAGE"
#  database = "INBOUND_INTEGRATION"
#  schema   = "LANDING_WEATHER"
#}
