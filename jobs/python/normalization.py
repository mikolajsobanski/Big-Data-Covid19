import pandas as pd
import json
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("CORD19-ETL").getOrCreate()

metadata_spark_df = spark.createDataFrame(metadata_df)
json_spark_df = spark.createDataFrame(json_df)

authors_df = metadata_spark_df.select(
    col('authors').alias('author')
).distinct()


publications_df = metadata_spark_df.select(
    col('title').alias('publication_title'),
    col('doi').alias('publication_doi'),
    col('journal').alias('publication_journal')
).distinct()


diseases_df = json_spark_df.select(
    col('diseases').alias('disease')
).distinct()
