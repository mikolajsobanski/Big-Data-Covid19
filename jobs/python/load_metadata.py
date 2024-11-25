import sys
from pyspark.sql import SparkSession
import pandas as pd

# Initialize Spark session
spark = SparkSession.builder \
    .appName("COVID-19_metadata") \
    .getOrCreate()

# Paths to the metadata CSV and JSON folder within the container
metadata_path = "/data/archive/metadata.csv"
metadata_sample_path = "/data/sample_metadata_1000.csv"

metadata_path = sys.argv[1] 
output_metadata_path = sys.argv[2]
partition_number = int(sys.argv[3])

# Load metadata CSV
metadata_df = spark.read.csv(metadata_sample_path, header=True, inferSchema=True)
metadata_df.show()

# Wirite metadata to bronze layer
output_matadata = metadata_df.repartition(partition_number)
output_matadata.write.csv("/data/bronze/metadata.csv", header=True, mode="overwrite")


spark.stop()