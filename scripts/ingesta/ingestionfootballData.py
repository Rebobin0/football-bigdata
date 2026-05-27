import requests
import boto3
import json
import time
from datetime import datetime

# CONFIGURACIÓN
API_KEY = "1c1e9e4497894fa286316dfaf2558d9f"
BASE_URL = "https://api.football-data.org/v4/competitions"
HEADERS = {
    "X-Auth-Token": API_KEY
}
BUCKET_NAME = "football-data-lake-22030108"

LEAGUES = {
    "PL": "premier_league",
    "PD": "la_liga",
    "BL1": "bundesliga",
    "SA": "serie_a",
    "FL1": "ligue_1"
}

s3 = boto3.client("s3")

# FUNCIÓN DE INGESTA
def fetch_teams(league_code):

    #url = f"{BASE_URL}/{league_code}/teams"
    #url = f"{BASE_URL}/{league_code}/standings"
    url = f"{BASE_URL}/{league_code}/matches"

    response = requests.get(url, headers=HEADERS)

    if response.status_code == 429:
        print("Rate limit alcanzado...")
        time.sleep(60)
        return fetch_teams(league_code)

    if response.status_code != 200:
        print(f"Error API {league_code}: {response.status_code}")
        return None

    return response.json()

# EXPORTAR A S3
for league_code, league_name in LEAGUES.items():

    print(f"\nProcesando {league_name}")

    data = fetch_teams(league_code)

    if not data:
        continue

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    filename = f"{league_name}_teams_{timestamp}.json"

    #s3_key = f"raw/football_data_org/teams/{league_name}/{filename}"

    s3_key = (
        f"raw/football_data_org/matches/"
        f"{league_name}/"
        f"{filename}"
    )

    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=s3_key,
        Body=json.dumps(data),
        ContentType="application/json"
    )

    print(f"Archivo subido a S3:")
    print(f"s3://{BUCKET_NAME}/{s3_key}")

print("\nProceso terminado.")