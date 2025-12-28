resource "snowflake_table" "trip_analytics" {
  name      = "TRIP_ANALYTICS"
  database  = snowflake_database.outbound_integration.name
  schema    = snowflake_schema.trip_analytics.name

  depends_on = [
    snowflake_schema.trip_analytics
  ]
  comment   = "Final consolidated table with trip and weather data"

  column {
    name = "vendor_id"
    type = "STRING"
  }

  column {
    name = "pickup_address"
    type = "STRING"
  }

  column {
    name = "drop_address"
    type = "STRING"
  }

  column {
    name = "pickup_time"
    type = "STRING"
  }
  column {
    name = "dropoff_time"
    type = "STRING"
  }
  column {
    name = "trip_distance"
    type = "STRING"
  }
  column {
    name = "total_amount"
    type = "STRING"
  }
  column {
    name = "tmin"
    type = "STRING"
  }
  column {
    name = "tmax"
    type = "STRING"
  }
  column {
    name = "prcp"
    type = "STRING"
  }
  column {
    name = "snow"
    type = "STRING"
  }
  column {
    name = "snwd"
    type = "STRING"
  }
  column {
    name = "awnd"
    type = "STRING"
  }
  column {
    name = "wsf2"
    type = "STRING"
  }
  column {
    name = "wdf2"
    type = "STRING"
  }
  column {
    name = "wsf5"
    type = "STRING"
  }
  column {
    name = "wdf5"
    type = "STRING"
  }
   column {
    name = "created_at"
    type = "TIMESTAMP_NTZ"
  }

  column {
    name = "created_by"
    type = "STRING"
  }
}