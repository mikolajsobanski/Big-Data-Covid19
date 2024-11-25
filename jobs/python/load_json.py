import sys
from pyspark.sql import SparkSession
import pandas as pd

# Initialize Spark session
spark = SparkSession.builder \
    .appName("COVID-19_Json") \
    .getOrCreate()

# Paths to the metadata CSV and JSON folder within the container
json_folder_path = "/data/archive/noncomm_use_subset/noncomm_use_subset/pdf_json"


json_folder_path = sys.argv[1] 
output_json_path = sys.argv[2] 
partition_number = int(sys.argv[3])


# Load JSON files from the specified folder
# Here we will read all JSON files in the folder and load them into a Spark DataFrame
json_df = spark.read.option("multiline", "true").json(json_folder_path)

# Show the schema of the JSON data (useful for debugging and inspection)
json_df.printSchema()

# Write JSON data to the bronze layer with partitioning
output_json = json_df.repartition(partition_number)
output_json.write.json(output_json_path, mode="overwrite")


spark.stop()