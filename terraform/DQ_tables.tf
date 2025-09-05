resource "snowflake_table" "trip_data_null_records" {
  name      = "TRIP_DATA_NULL_RECORDS"
  database  = "INBOUND_INTEGRATION"
  schema    = "DQ_TRIP"
  comment   = "Rows with Null values for YELLOW_TRIP_RECORDS Table"

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

resource "snowflake_table" "TRIP_DATA_DUPLICATES" {
  name      = "TRIP_DATA_DUPLICATES"
  database  = "INBOUND_INTEGRATION"
  schema    = "DQ_TRIP"
  comment   = "Rows with duplicate values for YELLOW_TRIP_RECORDS Table"

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

resource "snowflake_table" "INVALID_TRIP_DATA" {
  name      = "INVALID_TRIP_DATA"
  database  = "INBOUND_INTEGRATION"
  schema    = "DQ_TRIP"
  comment   = "Rows with junk values for YELLOW_TRIP_RECORDS Table"

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

resource "snowflake_table" "weather_null_records" {
  name      = "WEATHER_NULL_RECORDS"
  database  = "INBOUND_INTEGRATION"
  schema    = "DQ_WEATHER"
  comment   = "Null records for key columns from NYC Weather Data Table - unpivoted"

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

resource "snowflake_table" "WEATHER_DATA_DUPLICATES" {
  name      = "WEATHER_DATA_DUPLICATES"
  database  = "INBOUND_INTEGRATION"
  schema    = "DQ_WEATHER"
  comment   = "Duplicate records for key columns from NYC Weather Data Table - unpivoted"

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

resource "snowflake_table" "INVALID_WEATHER_DATA" {
  name      = "INVALID_WEATHER_DATA"
  database  = "INBOUND_INTEGRATION"
  schema    = "DQ_WEATHER"
  comment   = "Junk records for key columns from NYC Weather Data Table - unpivoted"

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