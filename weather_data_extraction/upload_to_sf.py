import os
from snowflake.connector import connect

conn = connect(
    user=os.environ["SF_USER"],
    password=os.environ["SF_PASSWORD"],
    account=os.environ["SF_ACCOUNT"],
)

cursor = conn.cursor()

file = sorted(os.listdir("data"))[-1]
file_path = f"data/{file}"

print(f"Uploading {file} to Snowflake stage...")

cursor.execute(f"""
PUT file://{file_path}
  @INBOUND_INTEGRATION.LANDING_WEATHER.LANDING_WEATHER_STAGE
  AUTO_COMPRESS=TRUE
""")

print("Weather file uploaded to Snowflake stage")