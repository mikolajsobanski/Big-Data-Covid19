from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count
from datetime import datetime

if __name__ == "__main__":
    import sys
    input_effects_path = sys.argv[1]  # Ścieżka do danych z warstwy srebrnej
    output_summary_path = sys.argv[2]  # Ścieżka do wyników

    # Inicjalizacja sesji Spark
    spark = SparkSession.builder.appName("Count COVID Effects").getOrCreate()

    # Wczytanie danych z warstwy srebrnej
    effects_df = spark.read.csv(input_effects_path, header=True)

    # Grupowanie i zliczanie wystąpień skutków COVID-19
    effect_summary_df = (
        effects_df.groupBy("category", "effect")
        .agg(count("*").alias("occurrences"))
        .orderBy(col("occurrences").desc())  # Sortowanie po liczbie wystąpień
    )

    # Generowanie timestampu i dodanie go do ścieżki
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    output_with_timestamp = f"{output_summary_path}_timestamp_{timestamp}"

    # Zapisanie wyników do warstwy złotej z timestampem w nazwie folderu
    effect_summary_df.write.csv(output_with_timestamp, header=True, mode="overwrite")

    # Zatrzymanie sesji Spark
    spark.stop()
