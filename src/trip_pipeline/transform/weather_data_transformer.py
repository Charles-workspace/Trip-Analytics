from snowflake.snowpark import Session
from snowflake.snowpark.functions import col,min,to_date
from src.trip_pipeline.configs.connection_config import connection_parameters

valid_weather_data = "INBOUND_INTEGRATION.SDS_WEATHER.WEATHER_DATA_VALIDATED"

def pivot_weather_table():
    session = Session.builder.configs(vars(connection_parameters)).create()
    df = session.table(valid_weather_data)
    
    datatypes = [row[0] for row in df.select ("datatype").distinct().collect()]
    #print (datatypes[0])
    #pivoted_df = df.pivot(datatypes).on("value").group_by("date").agg()

    pivoted_df = (
        df.group_by("DATE")                       # <- group first
        .pivot("datatype",datatypes)              # <- then pivot on DATATYPE values
        .agg(min(col("VALUE")))             # <- single aggregate is fine (and typical)
    )


    pivoted_df=pivoted_df.with_column('"ODate"',to_date(col("DATE"))
                                    ).with_column_renamed("'TMIN'",'"Tmin"'
                                    ).with_column_renamed("'TMAX'",'"Tmax"'
                                    ).with_column_renamed("'PRCP'",'"Prcp"'
                                    ).with_column_renamed("'SNOW'",'"Snow"'
                                    ).with_column_renamed("'SNWD'",'"Snwd"'
                                    ).with_column_renamed("'AWND'",'"Awnd"'
                                    ).with_column_renamed("'WSF5'",'"Wsf5"'
                                    ).with_column_renamed("'WDF5'",'"Wdf5"'
                                    ).with_column_renamed("'WSF2'",'"Wsf2"'
                                    ).with_column_renamed("'WDF2'",'"Wdf2"')

    return pivoted_df.select('"ODate"','"Tmin"','"Tmax"','"Prcp"','"Snow"','"Snwd"','"Awnd"','"Wsf5"','"Wdf5"','"Wsf2"','"Wdf2"')#,"'WT01'","'WT02'","'WT03'","'WT08'")
