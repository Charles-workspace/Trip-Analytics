resource "snowflake_stage" "landing_trip_stage" {
  name     = "LANDING_TRIP_STAGE"
  database = "INBOUND_INTEGRATION"
  schema   = "LANDING_TRIP"
}

resource "snowflake_stage" "trip_analytics_container_stage" {
  name     = "TRIP_ANALYTICS_CONTAINER_STAGE"
  database = "OPS"
  schema   = "SPCS_DEPLOYMENT"
}
