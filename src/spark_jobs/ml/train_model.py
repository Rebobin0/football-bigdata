import os
from pyspark.sql.functions import col, round as spark_round, when
from pyspark.ml.functions import vector_to_array
from src.spark_jobs.utils.s3_paths import s3_path
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from src.spark_jobs.utils.spark_session import create_spark_session
from src.spark_jobs.utils.s3_paths import PROCESSED_ML_TRAINING_DATASET

def main():
    spark = create_spark_session("Entrenamiento de Modelo ML - Partidos")
    print(f"\nLeyendo datos")
    df = spark.read.parquet(PROCESSED_ML_TRAINING_DATASET)
    features_cols = [
        # Financieras
        "home_squad_market_value_eur", 
        "away_squad_market_value_eur", 
        "market_value_diff_eur",
        "home_avg_player_market_value_eur",
        "away_avg_player_market_value_eur",
        "home_players_count",
        "away_players_count",
        # Deportivas (Nuevas)
        "home_rank",
        "away_rank",
        "home_points",
        "away_points",
        "home_goal_diff",
        "away_goal_diff"
    ]
    df_clean = df.dropna(subset=features_cols + ["target"])
    assembler = VectorAssembler(
        inputCols=features_cols,
        outputCol="features"
    )
    df_assembled = assembler.transform(df_clean)

    train_data, test_data = df_assembled.randomSplit([0.8, 0.2], seed=42)

    print("\n ENTRENANDO MODELO RANDOM FOREST")
    rf = RandomForestClassifier(
        featuresCol="features", 
        labelCol="target", 
        numTrees=100, 
        maxDepth=5, 
        seed=42
    )
    model = rf.fit(train_data)

    predictions = model.transform(test_data)
    print("\n=== EVALUACIÓN DEL MODELO ===")
    evaluator_acc = MulticlassClassificationEvaluator(
        labelCol="target", predictionCol="prediction", metricName="accuracy"
    )
    accuracy = evaluator_acc.evaluate(predictions)
    
    evaluator_f1 = MulticlassClassificationEvaluator(
        labelCol="target", predictionCol="prediction", metricName="f1"
    )
    f1_score = evaluator_f1.evaluate(predictions)

    print(f"Accuracy: {accuracy:.4f}")
    print(f"F1 Score: {f1_score:.4f}")

    print("\nVariables")
    importances = model.featureImportances
    for i, feature in enumerate(features_cols):
        print(f"{feature}: {importances[i]:.4f}")

    # ... (tu código que imprime los feature importances) ...

    print("\n=== GENERANDO PREDICCIONES FUTURAS PARA MONGODB ===")
    PROCESSED_ML_FUTURE_DATASET = s3_path("processed/ml/future_dataset/")

    # 1. Leer los partidos que se van a jugar próximamente
    df_future = spark.read.parquet(PROCESSED_ML_FUTURE_DATASET)

    # 2. Limpiar (quitar partidos donde falte info de Transfermarkt o Standings)
    df_future_clean = df_future.dropna(subset=features_cols)

    # 3. Ensamblar características igual que en el entrenamiento
    df_future_assembled = assembler.transform(df_future_clean)

    # 4. PREDECIR usando el Bosque Aleatorio ya entrenado
    future_predictions = model.transform(df_future_assembled)

    # 5. Formatear la salida para que el backend la pueda usar fácil
    df_export = (
        future_predictions
        .select(
            col("match_date").cast("string").alias("fecha"),
            col("league").alias("liga"),
            col("home_team_name").alias("local"),
            col("away_team_name").alias("visitante"),
            col("prediction").alias("prediccion_id"),
            "probability"
        )
        # Extraemos las probabilidades matemáticas (que vienen en un arreglo oculto de Spark)
        .withColumn("prob_array", vector_to_array("probability"))
        .withColumn("prob_gana_local", spark_round(col("prob_array").getItem(0) * 100, 2))
        .withColumn("prob_empate", spark_round(col("prob_array").getItem(1) * 100, 2))
        .withColumn("prob_gana_visitante", spark_round(col("prob_array").getItem(2) * 100, 2))
        # Traducimos el 0, 1 y 2 a texto humano
        .withColumn(
            "pronostico",
            when(col("prediccion_id") == 0.0, "Gana Local")
            .when(col("prediccion_id") == 1.0, "Empate")
            .when(col("prediccion_id") == 2.0, "Gana Visitante")
        )
        .drop("probability", "prob_array", "prediccion_id")
    )

    df_export.show(10, truncate=False)

    # 6. Exportar como un único archivo JSON localmente
    import os
    ruta_absoluta = os.path.abspath("data/output/predicciones_mongo")
    output_path = f"file://{ruta_absoluta}"
    
    df_export.coalesce(1).write.mode("overwrite").json(output_path)

    print(f"\nEl JSON para la API está en la carpeta: {ruta_absoluta}")

    spark.stop()

if __name__ == "__main__":
    main()