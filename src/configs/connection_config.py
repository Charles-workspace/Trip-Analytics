import os
from dataclasses import dataclass
from dotenv import load_dotenv

@dataclass(frozen=True)
class SnowflakeConfig:
    account: str
    user: str
    password: str
    role: str
    database: str
    schema: str
load_dotenv("src/.env", override=True)
connection_parameters = SnowflakeConfig(
    account=os.environ["SNOWFLAKE_ACCOUNT_NAME"],
    user=os.environ["SNOWFLAKE_USER"],
    password=os.environ["SNOWFLAKE_PASSWORD"],
    role=os.environ["SNOWFLAKE_ROLE"],
    database="INBOUND_INTEGRATION",
    schema="LANDING_WEATHER")
