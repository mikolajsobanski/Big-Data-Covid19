import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, split, regexp_extract
import pandas as pd

# Initialize Spark session
spark = SparkSession.builder \
    .appName("COVID-19_load_to_silver") \
    .getOrCreate()

# Input paths and output paths
metadata_path = sys.argv[1]
json_path = sys.argv[2] 
output_author_path = sys.argv[3]
output_publication_path = sys.argv[4]
output_disease_path = sys.argv[5]

# Load metadata CSV
metadata_df = spark.read.csv(metadata_path, header=True, inferSchema=True)

# Load JSON files
json_df = spark.read.option("multiline", "true").json(json_path)

# --- Job 1: Normalizacja danych do tabeli "autor" ---
# Konwersja kolumny authors z STRING na ARRAY
authors_df = metadata_df.select(
    explode(split(col('authors'), ',\s*')).alias('author')  # Rozdzielenie wartości po przecinku
).distinct()

# Zapisanie danych autorów do warstwy srebrnej
authors_df.write.csv(output_author_path, header=True, mode="overwrite")

# --- Job 2: Normalizacja danych do tabeli "publikacja" ---
# Wyodrębnienie informacji o publikacjach
publications_df = metadata_df.select(
    col('title').alias('publication_title'),
    col('doi').alias('publication_doi'),
    col('journal').alias('publication_journal')
).distinct()

# Zapisanie danych publikacji do warstwy srebrnej
publications_df.write.csv(output_publication_path, header=True, mode="overwrite")

# --- Job 3: Normalizacja danych do tabeli "jednostka chorobowa" ---
# Analiza body_text w JSON

# Explode the body_text field to analyze paragraphs individually
diseases_df = json_df.select(explode(col("body_text")).alias("paragraph")) \
    .select(col("paragraph.text").alias("text")) \
    .withColumn("disease", regexp_extract(col("text"), r'\b(COVID-19|SARS|Influenza|Ebola|H1N1|MERS)\b', 0)) \
    .filter(col("disease") != "") \
    .select("disease") \
    .distinct()

# Zapisanie danych jednostek chorobowych do warstwy srebrnej
diseases_df.write.csv(output_disease_path, header=True, mode="overwrite")
# Stop Spark session
spark.stop()
