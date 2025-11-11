from snowflake.snowpark.functions import col, lit, trim, row_number, try_cast
from snowflake.snowpark.window import Window
from snowflake.snowpark.types import IntegerType,TimestampType

class DQCheck :

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
            condition = col(column).is_null() |  (trim(col(column)) == lit(""))
            null_condition = condition if null_condition is None else null_condition | condition
        return null_condition
    

    def null_check(self,df,dq_table_name):
        """
        Checks for null/empty values in critical composite key columns
        and stores violating rows in a DQ table.
        """
        cond = self.build_null_condition()

        #print(cond)

        print ("Invalid rows with null/empty values in key columns")
        df_invalid = df.filter(cond)
        df_valid = df.filter(~cond)
        print("Valid rows with no null/empty values in key columns")
        df_valid.show()

        print("Total:", df.count())
        print("Invalid (nulls):", df.filter(cond).count())
        print("Valid:", df.filter(~cond).count())
        if df_invalid.count() > 0:
            print(f"Found {df_invalid.count()} rows with null/empty values in key columns.")
            df_invalid.write.mode("overwrite").save_as_table(dq_table_name)

        else:
            print("No null/empty values found in key columns.")
        return df_invalid.count(), df_valid
    
    def duplicate_check(self, df, dq_table_name):
        """
        Checks for duplicate rows based on composite key columns
        and stores violating rows in a DQ table.
        """

        # introduce row number to append only the first occurence of the duplicated records to clean and the rest to duplicates tables
        df_full_dedup = df.with_column('rn', 
            (row_number().over(Window.partition_by([col(c) for c in self.key_columns]).order_by([col(c) for c in self.key_columns]))))
        df_full_dedup_clean = df_full_dedup.filter(col('rn') == 1)
        df_full_dedup_clean = df_full_dedup_clean.drop('rn')
        df_full_dedup = df_full_dedup.filter(col('rn') != 1).drop('rn')

        clean_count = df_full_dedup_clean.count()
        print (f"{clean_count} clean records")

        dupe_count = df_full_dedup.count()
        
        if dupe_count > 0:
            print(f"{dupe_count} duplicate rows found. Recording to {dq_table_name}")
            df_full_dedup.write.mode("overwrite").save_as_table(dq_table_name)
        else:
            print("Duplicate check passed: No duplicate found")

        return df_full_dedup_clean

    def junk_value_check(self, df, columns, dtype, min_valid_epoch=1600000000000):
        """
        Flags rows where timestamp fields are present but invalid:
        - Either not castable to integer
        - Or below a minimum valid epoch threshold
        """
        junk_condition = None

        for column in columns:
            trimmed_col = trim(col(column))
            casted_col = try_cast(trimmed_col, IntegerType())
            if dtype == "timestamp_type":
                condition = ((casted_col.is_null()) | (casted_col < min_valid_epoch))
            else :
                condition = (casted_col.is_null())

            junk_condition = condition if junk_condition is None else (junk_condition | condition)

        return df.filter(junk_condition),df.filter(~junk_condition)
    
    def weather_junk_val_check (self,df,columns,dtype,weather_data_types):
        junk_condition = None

        for column in columns:
            trimmed_col = trim(col(column))
            
            if dtype == "timestamp_type":
                casted_col = try_cast(trimmed_col, TimestampType())
                condition = (casted_col.is_null())
                             
            elif dtype == "datatype_check":
                condition = ~trimmed_col.isin(weather_data_types)
                
            else:
                print ("Unrecognized dtype")

            junk_condition = condition if junk_condition is None else (junk_condition | condition)

        return df.filter(junk_condition),df.filter(~junk_condition)
            