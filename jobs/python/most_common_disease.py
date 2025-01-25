from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

'''
Odpowiedź na pytanie: Jaka jednostka chorobowa oprócz COVID-19 występowała najczęściej, z uwzględnieniem kategorii?
'''

if __name__ == "__main__":
    import sys
    input_disease_path = sys.argv[1]  # Ścieżka do danych wejściowych (disease, category)
    output_path = sys.argv[2]         # Ścieżka do danych wyjściowych

    # Inicjalizacja sesji Spark
    spark = SparkSession.builder.appName("Most Common Disease with Category").getOrCreate()

    # Wczytanie danych chorób
    diseases_df = spark.read.csv(input_disease_path, header=True)

    # Grupowanie i zliczanie wystąpień chorób, wykluczając COVID-19
    common_diseases_with_categories_df = (
        diseases_df.filter(col("disease") != "COVID-19")  # Wyklucz COVID-19
        .groupBy("disease", "category")                  # Grupowanie po chorobie i kategorii
        .agg(count("*").alias("occurrences"))            # Zliczanie wystąpień
        .orderBy(col("occurrences").desc())              # Sortowanie malejąco po liczbie wystąpień
    )

    # Zapisanie wyników
    common_diseases_with_categories_df.write.csv(output_path, header=True, mode="overwrite")

    # Zatrzymanie sesji Spark
    spark.stop()
