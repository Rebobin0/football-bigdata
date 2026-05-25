import os
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
        "home_squad_market_value_eur", 
        "away_squad_market_value_eur", 
        "market_value_diff_eur",
        "home_avg_player_market_value_eur",
        "away_avg_player_market_value_eur",
        "home_players_count",
        "away_players_count"
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

    spark.stop()

if __name__ == "__main__":
    main()