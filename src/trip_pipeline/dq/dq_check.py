from snowflake.snowpark.functions import col, lit, trim, row_number, try_cast, current_timestamp
from snowflake.snowpark.window import Window
from snowflake.snowpark.types import IntegerType,TimestampType
from trip_pipeline.configs.data_objects import config

class DQCheck :
    """
    This module defines the utilities for DQ checks which include performing basic
    checks such as null checks, duplicate checks and junk value checks on the sourced data,
    both trip and weather.
    
    Arguments:
    session : Snowpark Session object
    key_columns : List of critical key columns to be used for DQ checks
    dq_table_name : Name of the table to write the DQ failed records 
    specific to the source data.

    Method:
    The data that passes all the DQ checks is returned 
    as a DataFrame for further processing.
    """

    def __init__(self,session,key_columns, dq_table_name ):
        self.session = session
        self.key_columns = key_columns
        self.dq_table_name = dq_table_name

    def build_null_condition(self):
        """
        Builds a condition to check for null or empty values in key columns.
        """
        null_condition = None
        for column in self.key_columns:
            col_str = trim(col(column).cast("string"))
            is_null = col_str.is_null()
            is_empty = col_str == lit("")
            is_null_string = col_str.isin("", "NULL")

            condition = is_null | is_empty | is_null_string
            null_condition = condition if null_condition is None else null_condition | condition
        return null_condition

    def null_check(self,df,dq_table_name):
        """
        Checks for null/empty values in critical composite key columns
        and stores violating rows in a DQ table.
        """
        cond = self.build_null_condition()
        total_count = df.count()

        df_invalid = df.filter(cond)
        df_valid = df.filter(~cond)

        valid_count = df_valid.count()
        invalid_count = df_invalid.count()

        print("Total:", total_count)
        print("Invalid (nulls):", valid_count)
        print("Valid:", invalid_count)

        if df_invalid.count() > 0:
            print(f"Found {invalid_count} rows with null/empty values in key columns.")
            df_invalid.write.mode("overwrite").save_as_table(dq_table_name)

        else:
            print("No null/empty values found in key columns.")
        return invalid_count, df_valid
    
    def duplicate_check(self, df, dq_table_name):
        """
        Checks for duplicate rows based on composite key columns
        and stores violating rows in a DQ table.
        """

        # introduce row number to append only the first occurence of the duplicated 
        # records to clean and the rest to duplicates tables
        df_full_dedup = df.with_column(
            'rn',
            row_number().over(
                Window
                .partition_by([col(c) for c in self.key_columns])
                .order_by([col(c) for c in self.key_columns])
            )
        )

        df_clean = df_full_dedup.filter(col('rn') == 1).drop('rn')
        df_dupes = (
            df_full_dedup
            .filter(col('rn') != 1)
            .drop('rn')
            .with_column("dq_ts", current_timestamp())
        )

        dupe_count = df_dupes.count()
        
        if dupe_count > 0:
            print(f"{dupe_count} duplicate rows found. Recording to {dq_table_name}")
            df_dupes.write.mode("overwrite").save_as_table(dq_table_name)
        else:
            print("Duplicate check passed: No duplicate found")

        return df_clean

    def validate_trip_data(self, df, columns, dtype, min_valid_epoch=config.min_valid_epoch):
        """
        Flags rows where timestamp fields are present but invalid:
        - Either not castable to integer
        - Or with invalid epoch values
        """
        junk_condition = None

        for column in columns:
            trimmed_col = trim(col(column))
            casted_col = try_cast(trimmed_col, IntegerType())
            if dtype == "timestamp_type":
                condition = ((casted_col.is_null()) | (casted_col < min_valid_epoch))
            else :
                condition = casted_col.is_null()

            junk_condition = condition if junk_condition is None else (junk_condition | condition)

        return df.filter(junk_condition),df.filter(~junk_condition)

    def validate_weather_data (self,df,columns,dtype,weather_data_types):
        junk_condition = None

        for column in columns:
            trimmed_col = trim(col(column))
            condition = None
            
            if dtype == "timestamp_type":
                casted_col = try_cast(trimmed_col, TimestampType())
                condition = casted_col.is_null()
            elif dtype == "datatype_check":
                condition = ~trimmed_col.isin(weather_data_types)
            else:
                raise ValueError(f"Unsupported dtype for junk check: {dtype}")

            junk_condition = (
                condition if junk_condition is None else junk_condition | condition
            )

        return df.filter(junk_condition),df.filter(~junk_condition)