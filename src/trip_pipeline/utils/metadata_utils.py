from snowflake.snowpark.functions import lit, current_timestamp
from trip_pipeline.configs.data_objects import config

from snowflake.snowpark.functions import lit, current_timestamp

def add_etl_metadata_dq(df, rule_name, error_message=None):
    df = (
        df
        .with_column("dq_failure_type", lit(rule_name))
        .with_column("error_description", lit(error_message) if error_message else lit(None))
        .with_column("created_at", current_timestamp())
        .with_column("created_by", lit (config.ETL_created_by))
    )
    return df

def add_etl_metadata_clean(df):
    df = (
        df
        .with_column("created_at", current_timestamp())
        .with_column("created_by", lit (config.ETL_created_by))
    )
    return df