from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

if __name__ == "__main__":
    import sys
    input_strains_path = sys.argv[1]  # Ścieżka do danych z warstwy srebrnej
    output_summary_path = sys.argv[2]  # Ścieżka do wyników

    spark = SparkSession.builder.appName("Count Coronavirus Strains").getOrCreate()

    # Wczytanie danych z warstwy srebrnej
    strains_df = spark.read.csv(input_strains_path, header=True)

    # Grupowanie i zliczanie wystąpień szczepów koronawirusa
    strain_summary_df = (
        strains_df.groupBy("category", "strain")
        .agg(count("*").alias("occurrences"))
        .orderBy(col("occurrences").desc())
    )

    # Zapisanie wyników do warstwy złotej
    strain_summary_df.write.csv(output_summary_path, header=True, mode="overwrite")

    spark.stop()
