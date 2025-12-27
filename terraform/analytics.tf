resource "snowflake_table" "trip_analytics" {
  name      = "TRIP_ANALYTICS"
  database  = snowflake_database.outbound_integration.name
  schema    = snowflake_schema.trip_analytics.name

  depends_on = [
    snowflake_schema.trip_analytics
  ]
  comment   = "Final consolidated table with trip and weather data"

  column {
    name = "VendorID"
    type = "STRING"
  }

  column {
    name = "PickupAddress"
    type = "STRING"
  }

  column {
    name = "DropAddress"
    type = "STRING"
  }

  column {
    name = "PickupTime"
    type = "STRING"
  }
  column {
    name = "DropoffTime"
    type = "STRING"
  }
  column {
    name = "TripDistance"
    type = "STRING"
  }
  column {
    name = "TotalAmount"
    type = "STRING"
  }
  column {
    name = "Tmin"
    type = "STRING"
  }
  column {
    name = "Tmax"
    type = "STRING"
  }
  column {
    name = "Prcp"
    type = "STRING"
  }
  column {
    name = "Snow"
    type = "STRING"
  }
  column {
    name = "Snwd"
    type = "STRING"
  }
  column {
    name = "Awnd"
    type = "STRING"
  }
  column {
    name = "Wsf2"
    type = "STRING"
  }
  column {
    name = "Wdf2"
    type = "STRING"
  }
  column {
    name = "Wsf5"
    type = "STRING"
  }
  column {
    name = "Wdf5"
    type = "STRING"
  }
   column {
    name = "CREATED_AT"
    type = "TIMESTAMP_NTZ"
  }

  column {
    name = "CREATED_BY"
    type = "STRING"
  }
}