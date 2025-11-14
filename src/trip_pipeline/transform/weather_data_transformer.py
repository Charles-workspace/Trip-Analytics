from snowflake.snowpark.functions import col,min,to_date

valid_weather_data = "INBOUND_INTEGRATION.SDS_WEATHER.WEATHER_DATA_VALIDATED"

def pivot_weather_table(session):
    
    df = session.table(valid_weather_data)
    
    datatypes = [row[0] for row in df.select ("datatype").distinct().collect()]
    #print (datatypes[0])
    #pivoted_df = df.pivot(datatypes).on("value").group_by("date").agg()

    pivoted_df = (
        df.group_by("DATE")                     
        .pivot("datatype",datatypes)  
        .agg(min(col("VALUE"))) 
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
