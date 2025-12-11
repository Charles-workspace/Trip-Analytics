from snowflake.snowpark.functions import col,min,to_date

#valid_weather_data = "INBOUND_INTEGRATION.SDS_WEATHER.WEATHER_DATA_VALIDATED"

def clean_weather_columns(df):
    rename_map = {}

    for c in df.columns:
        clean = (
            c.replace('"', '')   # remove inner double quotes
             .replace("'", "")   # remove single quotes
             .strip()
             .lower()
        )
        rename_map[c] = clean

    for old, new in rename_map.items():
        if old != new:
            df = df.with_column_renamed(old, new)
    return df


def pivot_weather_table(session, valid_weather_data):

    df = session.table(valid_weather_data)

    # CLEAN COLUMN NAMES FIRST
    df = clean_weather_columns(df)

    datatypes = [row[0] for row in df.select("datatype").distinct().collect()]

    # Pivot on clean names
    pivoted_df = (
        df.group_by("date")
          .pivot("datatype", datatypes)
          .agg(min(col("value")))
    )
    pivoted_df = clean_weather_columns(pivoted_df)

    pivoted_df = pivoted_df.with_column("o_date", to_date(col("date")))

    return pivoted_df.select(
        "o_date",
        "tmin", "tmax", "prcp",
        "snow", "snwd", "awnd",
        "wsf2", "wdf2", "wsf5", "wdf5"
    )