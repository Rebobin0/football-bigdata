import os
import io
import json
import boto3
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

S3_BUCKET = os.getenv("S3_BUCKET")
TRAINING_PREFIX = "processed/ml/training_dataset/"
OUTPUT_DIR = "data/output"

HOME_RENAME = {
    "home_team_id": "team_id",
    "home_team_name": "team_name",
    "home_squad_market_value_eur": "squad_market_value_eur",
    "home_avg_player_market_value_eur": "avg_player_market_value_eur",
    "home_players_count": "players_count",
    "home_rank": "rank",
    "home_points": "points",
    "home_goal_diff": "goal_diff",
}
AWAY_RENAME = {
    "away_team_id": "team_id",
    "away_team_name": "team_name",
    "away_squad_market_value_eur": "squad_market_value_eur",
    "away_avg_player_market_value_eur": "avg_player_market_value_eur",
    "away_players_count": "players_count",
    "away_rank": "rank",
    "away_points": "points",
    "away_goal_diff": "goal_diff",
}
TEAM_COLS = ["team_id", "team_name", "league", "season",
             "squad_market_value_eur", "avg_player_market_value_eur",
             "players_count", "rank", "points", "goal_diff"]


def read_parquet_from_s3(bucket: str, prefix: str) -> pd.DataFrame:
    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")
    dfs = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith(".parquet"):
                continue
            print(f"  Descargando: {key}")
            response = s3.get_object(Bucket=bucket, Key=key)
            df = pd.read_parquet(io.BytesIO(response["Body"].read()))
            # Spark escribe las columnas de partición en el path, no dentro del archivo.
            # Las extraemos del formato "clave=valor" de cada segmento del key.
            for segment in key.split("/"):
                if "=" in segment:
                    col_name, col_val = segment.split("=", 1)
                    df[col_name] = col_val
            dfs.append(df)
    if not dfs:
        raise FileNotFoundError(f"No se encontraron archivos Parquet en s3://{bucket}/{prefix}")
    return pd.concat(dfs, ignore_index=True)


def build_team_stats(df: pd.DataFrame) -> pd.DataFrame:
    df_home = df.rename(columns=HOME_RENAME)[["league", "season"] + list(HOME_RENAME.values())].dropna()
    df_away = df.rename(columns=AWAY_RENAME)[["league", "season"] + list(AWAY_RENAME.values())].dropna()

    df_teams = pd.concat([df_home, df_away], ignore_index=True)

    df_teams["season"] = df_teams["season"].astype(int)
    df_latest = (
        df_teams
        .sort_values("season", ascending=False)
        .drop_duplicates(subset=["team_id", "league"])
        .drop(columns=["season"])
        .reset_index(drop=True)
    )
    return df_latest


def main():
    if not S3_BUCKET:
        raise ValueError("S3_BUCKET no está definido en el archivo .env")

    print(f"Leyendo dataset de entrenamiento desde s3://{S3_BUCKET}/{TRAINING_PREFIX}")
    df = read_parquet_from_s3(S3_BUCKET, TRAINING_PREFIX)
    print(f"Total filas leídas: {len(df)}")

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    training_path = os.path.join(OUTPUT_DIR, "training_data.parquet")
    df.to_parquet(training_path, index=False)
    print(f"\nDataset de entrenamiento guardado en: {training_path}")

    df_teams = build_team_stats(df)
    team_stats_path = os.path.join(OUTPUT_DIR, "team_stats.json")
    df_teams.to_json(team_stats_path, orient="records", indent=2)
    print(f"Estadísticas de {len(df_teams)} equipos guardadas en: {team_stats_path}")
    print("\nPrimeros equipos exportados:")
    print(df_teams[["team_name", "league", "rank", "points"]].head(10).to_string(index=False))


if __name__ == "__main__":
    main()
