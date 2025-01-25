from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, explode, split

'''
Odpowiedź na pytanie: Który autor miał najwięcej publikacji?
'''

if __name__ == "__main__":
    import sys
    input_author_path = sys.argv[1]
    input_publication_path = sys.argv[2]
    output_path = sys.argv[3]

    spark = SparkSession.builder.appName("Most Prolific Author").getOrCreate()

    # Wczytanie danych autorów i publikacji
    authors_df = spark.read.csv(input_author_path, header=True)
    publications_df = spark.read.csv(input_publication_path, header=True)

    # Rozdzielenie listy autorów w publikacjach
    author_publications_df = publications_df.select(
        explode(split(col("publication_authors"), ",\\s*")).alias("author")
    )

    # Zliczenie liczby publikacji dla każdego autora
    prolific_authors_df = (
        author_publications_df.groupBy("author")
        .agg(count("*").alias("publication_count"))
        .orderBy(col("publication_count").desc())
    )

    # Zapisanie wyników
    prolific_authors_df.write.csv(output_path, header=True, mode="overwrite")

    spark.stop()
