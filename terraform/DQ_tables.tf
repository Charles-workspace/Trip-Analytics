resource "snowflake_table" "trip_data_dq" {
  name      = "TRIP_DATA_DQ"
  database  = snowflake_database.inbound_integration.name
  schema    = snowflake_schema.dq_check_trip.name
  depends_on = [
    snowflake_schema.dq_check_trip
  ]
  comment   = "Records that failed DQ check for YELLOW_TRIP_RECORDS Table"

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

  column {
    name = "DQ_FAILURE_TYPE"
    type = "STRING"
  }

  column {
    name = "ERROR_DESCRIPTION"
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

resource "snowflake_table" "weather_data_dq" {
  name      = "WEATHER_DATA_DQ"
  database  = snowflake_database.inbound_integration.name
  schema    = snowflake_schema.dq_check_weather.name
  depends_on = [
    snowflake_schema.dq_check_weather
  ]
  comment   = "Records that failed DQ check from NYC Weather Data Table - unpivoted"

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

  column {
    name = "DQ_FAILURE_TYPE"
    type = "STRING"
  }

  column {
    name = "ERROR_DESCRIPTION"
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