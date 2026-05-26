import sys

from pyspark.ml.classification import RandomForestClassificationModel
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.sql.functions import col, lower, round as spark_round, when

from src.spark_jobs.utils.spark_session import create_spark_session
from src.spark_jobs.utils.s3_paths import (
    LOCAL_RANDOM_FOREST_MODEL,
    PROCESSED_TEAM_TRANSFERMARKT_MATCHES,
    PROCESSED_FACT_STANDINGS_FOOTBALL_DATA,
)


def get_team_data(df_market, df_standings, team_name):
    team_name_lower = team_name.lower()

    market = (
        df_market
        .filter(lower(col("team_name")).contains(team_name_lower))
        .orderBy(col("season").desc())
        .limit(1)
    )

    if market.count() == 0:
        return None

    market_row = market.collect()[0]

    standings = (
        df_standings
        .filter(
            (col("team_id") == market_row["team_id"]) &
            (col("league") == market_row["league"])
        )
        .orderBy(col("season").desc())
        .limit(1)
    )

    if standings.count() == 0:
        return None

    standing_row = standings.collect()[0]

    return {
        "team_id": market_row["team_id"],
        "team_name": market_row["team_name"],
        "league": market_row["league"],
        "squad_market_value_eur": market_row["squad_market_value_eur"],
        "avg_player_market_value_eur": market_row["avg_player_market_value_eur"],
        "players_count": market_row["players_count"],
        "rank": standing_row["rank"],
        "points": standing_row["points"],
        "goal_difference": standing_row["goal_difference"],
    }


def main():
    if len(sys.argv) != 3:
        print('\nUso:')
        print('python src/spark_jobs/ml/predict_custom_match.py "Equipo Local" "Equipo Visitante"')
        sys.exit(1)

    home_team = sys.argv[1]
    away_team = sys.argv[2]

    spark = create_spark_session("Predicción Custom Match")

    print("\nCargando modelo...")
    model = RandomForestClassificationModel.load(LOCAL_RANDOM_FOREST_MODEL)

    print("\nLeyendo datasets de equipos...")
    df_market = spark.read.parquet(PROCESSED_TEAM_TRANSFERMARKT_MATCHES)
    df_standings = spark.read.parquet(PROCESSED_FACT_STANDINGS_FOOTBALL_DATA)

    home = get_team_data(df_market, df_standings, home_team)
    away = get_team_data(df_market, df_standings, away_team)

    if home is None:
        print(f'\nNo encontré datos completos para el equipo local: {home_team}')
        df_market.select("team_name", "league", "season").orderBy("league", "team_name").show(100, truncate=False)
        spark.stop()
        sys.exit(1)

    if away is None:
        print(f'\nNo encontré datos completos para el equipo visitante: {away_team}')
        df_market.select("team_name", "league", "season").orderBy("league", "team_name").show(100, truncate=False)
        spark.stop()
        sys.exit(1)

    if home["league"] != away["league"]:
        print("\nAdvertencia: los equipos parecen ser de ligas diferentes.")
        print(f'{home["team_name"]}: {home["league"]}')
        print(f'{away["team_name"]}: {away["league"]}')

    features_cols = [
        "home_squad_market_value_eur",
        "away_squad_market_value_eur",
        "market_value_diff_eur",
        "home_avg_player_market_value_eur",
        "away_avg_player_market_value_eur",
        "home_players_count",
        "away_players_count",
        "home_rank",
        "away_rank",
        "home_points",
        "away_points",
        "home_goal_diff",
        "away_goal_diff"
    ]

    match_data = [{
        "home_team_name": home["team_name"],
        "away_team_name": away["team_name"],
        "league": home["league"],

        "home_squad_market_value_eur": float(home["squad_market_value_eur"]),
        "away_squad_market_value_eur": float(away["squad_market_value_eur"]),
        "market_value_diff_eur": float(home["squad_market_value_eur"] - away["squad_market_value_eur"]),

        "home_avg_player_market_value_eur": float(home["avg_player_market_value_eur"]),
        "away_avg_player_market_value_eur": float(away["avg_player_market_value_eur"]),

        "home_players_count": int(home["players_count"]),
        "away_players_count": int(away["players_count"]),

        "home_rank": int(home["rank"]),
        "away_rank": int(away["rank"]),
        "home_points": int(home["points"]),
        "away_points": int(away["points"]),
        "home_goal_diff": int(home["goal_difference"]),
        "away_goal_diff": int(away["goal_difference"]),
    }]

    df_match = spark.createDataFrame(match_data)

    assembler = VectorAssembler(
        inputCols=features_cols,
        outputCol="features"
    )

    df_assembled = assembler.transform(df_match)
    predictions = model.transform(df_assembled)

    df_result = (
        predictions
        .select(
            col("league").alias("liga"),
            col("home_team_name").alias("local"),
            col("away_team_name").alias("visitante"),
            col("prediction").alias("prediccion_id"),
            "probability"
        )
        .withColumn("prob_array", vector_to_array("probability"))
        .withColumn("prob_gana_local", spark_round(col("prob_array").getItem(0) * 100, 2))
        .withColumn("prob_empate", spark_round(col("prob_array").getItem(1) * 100, 2))
        .withColumn("prob_gana_visitante", spark_round(col("prob_array").getItem(2) * 100, 2))
        .withColumn(
            "pronostico",
            when(col("prediccion_id") == 0.0, "Gana Local")
            .when(col("prediccion_id") == 1.0, "Empate")
            .when(col("prediccion_id") == 2.0, "Gana Visitante")
        )
        .drop("probability", "prob_array", "prediccion_id")
    )

    print("\n=== PREDICCIÓN ===")
    df_result.show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()