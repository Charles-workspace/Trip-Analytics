resource "snowflake_table" "trip_data_sds" {
  name = "TRIP_DATA_VALIDATED"
  database = snowflake_database.inbound_integration.name
  schema    = snowflake_schema.staging_trip.name
  depends_on = [
    snowflake_schema.staging_trip
  ]
  comment = "Records that have passed DQ Checks"

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

resource "snowflake_table" "weather_data_sds" {
  name = "WEATHER_DATA_VALIDATED"
  database = snowflake_database.inbound_integration.name
  schema    = snowflake_schema.staging_weather.name
  depends_on = [
    snowflake_schema.staging_weather
  ]
  comment   = "DQ Validated NYC Weather Data  - unpivoted"

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

resource "snowflake_table" "weather_data_pivoted" {
  name      = "WEATHER_DATA_PIVOTED"
  database  = snowflake_database.inbound_integration.name
  schema    = snowflake_schema.staging_weather.name

  depends_on = [
    snowflake_schema.staging_weather
  ]

  comment = "Pivoted weather dataset retained after transformation"

  column { 
    name = "ODate" 
    type = "DATE" 
    }
  column { 
    name = "Tmin" 
    type = "FLOAT" 
    }
  column {
    name = "Tmax" 
    type = "FLOAT" 
    }
  column {
    name = "Prcp" 
    type = "FLOAT" 
    }
  column { 
    name = "Snow" 
    type = "FLOAT" 
    }
  column { 
    name = "Snwd" 
    type = "FLOAT" 
    }
  column { 
    name = "Awnd" 
    type = "FLOAT" 
    }
  column { 
    name = "Wsf2" 
    type = "FLOAT" 
    }
  column { 
    name = "Wdf2" 
    type = "FLOAT" 
    }
  column { 
    name = "Wsf5" 
    type = "FLOAT" 
    }
  column { 
    name = "Wdf5" 
    type = "FLOAT" 
    }
}