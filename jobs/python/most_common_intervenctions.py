from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

'''
Odpowiedź na pytanie: Jakie interwencje zdrowotne były najczęściej omawiane w badaniach dotyczących COVID-19?
'''



if __name__ == "__main__":
    import sys
    input_interventions_path = sys.argv[1]  # Ścieżka do danych z warstwy srebrnej
    output_summary_path = sys.argv[2]      # Ścieżka do wyników

    # Inicjalizacja sesji Spark
    spark = SparkSession.builder.appName("Count Health Interventions").getOrCreate()

    # Wczytanie danych z warstwy srebrnej
    interventions_df = spark.read.csv(input_interventions_path, header=True)

    # Grupowanie i zliczanie wystąpień słów kluczowych
    keyword_summary_df = (
        interventions_df.groupBy("category", "subcategory", "keyword")
        .agg(count("*").alias("occurrences"))
        .orderBy(col("occurrences").desc())
    )

    # Zapisanie wyników do warstwy złotej
    keyword_summary_df.write.csv(output_summary_path, header=True, mode="overwrite")

    spark.stop()
