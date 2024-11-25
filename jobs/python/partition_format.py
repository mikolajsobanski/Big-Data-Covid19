import sys
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("CORD-19 Data Partitioning and Formatting") \
    .getOrCreate()

# Retrieve arguments
metadata_path = sys.argv[1]
json_folder_path = sys.argv[2]
partition_metadata_path = sys.argv[3]
partition_number = int(sys.argv[4])

# Load metadata CSV
metadata_df = spark.read.csv(metadata_path, header=True, inferSchema=True)

# Repartition the metadata DataFrame
partitioned_metadata_df = metadata_df.repartition(partition_number)

# Save partitioned DataFrame to output path
partitioned_metadata_df.write.csv(partition_metadata_path, header=True, mode="overwrite")

# Stop Spark session
spark.stop()
