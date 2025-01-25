import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, udf, split, regexp_extract
from pyspark.sql.types import ArrayType, StringType
import pandas as pd
import spacy

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
# Ekstrakcja autorów z sekcji "metadata"
authors_df = json_df.select(
    explode(col("metadata.authors")).alias("author")  # Rozpakowanie listy autorów
).select(
    col("author.first").alias("first_name"),         # Imię autora
    col("author.last").alias("last_name")            # Nazwisko autora
).distinct()  # Usunięcie duplikatów

# Zapisanie danych do pliku CSV
authors_df.write.csv(output_author_path, header=True, mode="overwrite")
'''
# Konwersja kolumny authors z STRING na ARRAY
authors_df = metadata_df.select(
    explode(split(col('authors'), ',\s*')).alias('author')  # Rozdzielenie wartości po przecinku
).distinct()

# Zapisanie danych autorów do warstwy srebrnej
authors_df.write.csv(output_author_path, header=True, mode="overwrite")
'''
# --- Job 2: Normalizacja danych do tabeli "publikacja" ---
# Wyodrębnienie informacji o publikacjach
publications_df = metadata_df.select(
    col('cord_uid').alias('publication_cord_uid'),
    col('title').alias('publication_title'),
    col('abstract').alias('publication_abstract'),
    col('publish_time').alias('publication_publish_time'),
    col('authors').alias('publication_authors'),
).distinct()

# Zapisanie danych publikacji do warstwy srebrnej
publications_df.write.csv(output_publication_path, header=True, mode="overwrite")

spark.stop()

# --- Job 3: Normalizacja danych do tabeli "jednostka chorobowa" ---
# Analiza body_text w JSON

# Explode the body_text field to analyze paragraphs individually
# Ładowanie modelu SciSpaCy
'''
nlp = spacy.load("en_core_web_lg")

# Funkcja do wykrywania chorób za pomocą spaCy
def extract_diseases(text):
    doc = nlp(text)
    diseases = [ent.text for ent in doc.ents if ent.label_ == "DISEASE"]
    return diseases

# UDF (User Defined Function) dla Spark
extract_diseases_udf = udf(extract_diseases, ArrayType(StringType()))

# Przetwarzanie danych z użyciem spaCy
diseases_df = json_df.select(explode(col("body_text")).alias("paragraph")) \
    .select(col("paragraph.text").alias("text")) \
    .withColumn("diseases", extract_diseases_udf(col("text"))) \
    .select(explode(col("diseases")).alias("disease")) \
    .distinct()

'''

'''
diseases_df = json_df.select(explode(col("body_text")).alias("paragraph")) \
    .select(col("paragraph.text").alias("text")) \
    .withColumn("disease", regexp_extract(col("text"), r'\b(COVID-19|SARS|Influenza|Ebola|H1N1|MERS)\b', 0)) \
    .filter(col("disease") != "") \
    .select("disease") \
    .distinct()
'''
# Zapisanie danych jednostek chorobowych do warstwy srebrnej
#diseases_df.write.csv(output_disease_path, header=True, mode="overwrite")
# Stop Spark session
#spark.stop()
