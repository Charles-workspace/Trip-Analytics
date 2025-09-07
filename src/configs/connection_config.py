import os
from dataclasses import dataclass

@dataclass(frozen=True)
class SnowflakeConfig:
    account: str
    user: str
    password: str
    role: str
    database: str
    schema: str

connection_parameters = SnowflakeConfig(
    account=os.environ["snowflake_account_name"],
    user=os.environ["snowflake_user"],
    password=os.environ["snowflake_password"],
    role=os.environ["snowflake_role"],
    database="INBOUND_INTEGRATION",
    schema="LANDING_WEATHER")
