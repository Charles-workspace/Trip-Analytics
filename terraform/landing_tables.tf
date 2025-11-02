resource "snowflake_table" "taxi_zone_lookup" {
  name      = "TAXI_ZONE_LOOKUP"
  database = snowflake_database.inbound_integration.name
  schema    = snowflake_schema.landing_trip.name
  depends_on = [
    snowflake_schema.landing_trip
  ]
  comment   = "Taxi Zone Lookup Table"

  column {
    name = "LOCATIONID"
    type = "STRING"
  }

  column {
    name = "BOROUGH"
    type = "STRING"
  }

  column {
    name = "ZONE"
    type = "STRING"
  }

  column {
    name = "SERVICE_ZONE"
    type = "STRING"
  }
}

resource "snowflake_table" "yellow_trip_records" {
  name      = "YELLOW_TRIP_RECORDS"
  database = snowflake_database.inbound_integration.name
  schema    = snowflake_schema.landing_trip.name
  depends_on = [
    snowflake_schema.landing_trip
  ]
  comment   = "NYC Yellow Taxi Trip Records Table"

  column {
    name = "VendorID"
    type = "STRING"
  }

  column {
    name = "tpep_pickup_datetime"
    type = "STRING"
  }

  column {
    name = "tpep_dropoff_datetime"
    type = "STRING"
  }

  column {
    name = "passenger_count"
    type = "STRING"
  }

  column {
    name = "trip_distance"
    type = "STRING"
  }
  
  column {
    name = "RatecodeID"
    type = "STRING"
  }
  
  column {
    name = "store_and_fwd_flag"
    type = "STRING"
  }
  
  column {
    name = "PULocationID"
    type = "STRING"
  }
  
  column {
    name = "DOLocationID"
    type = "STRING"
  }
  
  column {
    name = "payment_type"
    type = "STRING"
  }
  
  column {
    name = "fare_amount"
    type = "STRING"
  }
  
  column {
    name = "extra"
    type = "STRING"
  }
  
  column {
    name = "mta_tax"
    type = "STRING"
  }
  
  column {
    name = "tip_amount"
    type = "STRING"
  }
  
  column {
    name = "tolls_amount"
    type = "STRING"
  }
  
  column {
    name = "improvement_surcharge"
    type = "STRING"
  }
  
  column {
    name = "total_amount"
    type = "STRING"
  }
  
  column {
    name = "congestion_surcharge"
    type = "STRING"
  }
  
  column {
    name = "Airport_fee"
    type = "STRING"
  }
  
  column {
    name = "cbd_congestion_fee"
    type = "STRING"
  }
}

resource "snowflake_table" "nyc_weather" {
  name      = "NYC_WEATHER"
  database = snowflake_database.inbound_integration.name
  schema    = snowflake_schema.landing_weather.name
  depends_on = [
    snowflake_schema.landing_weather
  ]
  comment   = "Raw NYC Weather Data Table - unpivoted"

  column {
    name = "date"
    type = "STRING"
  }

  column {
    name = "datatype"
    type = "STRING"
  }

  column {
    name = "station"
    type = "STRING"
  }

  column {
    name = "attributes"
    type = "STRING"
  }

  column {
    name = "value"
    type = "STRING"
  }
}
