import sys, requests, pandas as pd, os, json

TOKEN = os.environ["NOAA_TOKEN"]
start = sys.argv[1]
end   = sys.argv[2]

endpoint = "https://www.ncei.noaa.gov/cdo-web/api/v2/data"
headers = {"token": TOKEN}

params = {
    "datasetid": "GHCND",
    "stationid": "GHCND:USW00094728",
    "startdate": start,
    "enddate": end,
    "units": "metric",
    "limit": 1000
}

response = requests.get(endpoint, headers=headers, params=params)

if response.status_code != 200:
    raise Exception(f"NOAA API error {response.status_code} {response.text}")

df = pd.json_normalize(response.json()['results'])[["date","datatype","station","attributes","value"]]

os.makedirs("data", exist_ok=True)
output = f"data/weather_{start}_{end}.csv"
df.to_csv(output, index=False)

print("Weather data saved to", output)