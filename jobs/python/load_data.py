import sys
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("CORD-19 Data Processing - Version 8") \
    .getOrCreate()

# Paths to the metadata CSV and JSON folder within the container
metadata_path = "/data/archive/metadata.csv"
json_folder_path = "/data/archive/noncomm_use_subset/noncomm_use_subset/pdf_json"
metadata_sample_path = "/data/sample_metadata_1000.csv"

metadata_path = sys.argv[1] 
json_folder_path = sys.argv[2] 
output_metadata_path = sys.argv[3]
partition_number = int(sys.argv[4])

# Load metadata CSV
metadata_df = spark.read.csv(metadata_sample_path, header=True, inferSchema=True)
metadata_df.show()
# Load all JSON files as a single DataFrame
#publication_df = spark.read.json(f"{json_folder_path}/*.json")

output_matadata = metadata_df.repartition(partition_number)
output_matadata.write.csv("/data/bronze/metadata.csv", header=True, mode="overwrite")

spark.stop()