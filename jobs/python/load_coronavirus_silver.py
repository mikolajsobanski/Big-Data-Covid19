from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, udf
from pyspark.sql.types import ArrayType, StructType, StructField, StringType

# Kategorie i szczepy koronawirusów
categories = {
    "Alphacoronavirus": ["Alphacoronavirus", "HCoV-229E", "229E", "HCoV-NL63", "NL63"],
    "Betacoronavirus": ["Betacoronavirus", "SARS-CoV", "SARS", "MERS-CoV", "MERS", "SARS-CoV-2", "COVID-19", "2019-nCoV"],
    "Gammacoronavirus": ["Gammacoronavirus", "gamma coronavirus", "avian coronavirus", "IBV"],
    "Deltacoronavirus": ["Deltacoronavirus", "Delta-CoV", "Porcine Deltacoronavirus", "PDCoV"],
    "Human Coronaviruses": ["Human Coronaviruses", "HCoV-229E", "229E", "HCoV-NL63", "HCoV-OC43", "HCoV-HKU1"],
    "Animal Coronaviruses": ["Animal Coronaviruses", "bat coronavirus", "Bat-CoV", "camel coronavirus", "avian coronavirus", "IBV", "Porcine Deltacoronavirus", "PDCoV"],
}

# Funkcja do ekstrakcji szczepów koronawirusa
def extract_strains(text):
    result = []
    for category, strains in categories.items():
        for strain in strains:
            if strain.lower() in text.lower():
                result.append({"category": category, "strain": strain})
    return result

if __name__ == "__main__":
    import sys
    input_json_path = sys.argv[1]
    output_strains_path = sys.argv[2]

    spark = SparkSession.builder.appName("Extract Coronavirus Strains").getOrCreate()

    # Wczytanie danych JSON
    json_df = spark.read.option("multiline", "true").json(input_json_path)

    # Schemat dla UDF
    schema = ArrayType(
        StructType([
            StructField("category", StringType(), True),
            StructField("strain", StringType(), True)
        ])
    )

    # UDF dla Spark
    extract_strains_udf = udf(extract_strains, schema)

    # Ekstrakcja szczepów koronawirusa
    strains_df = json_df.select(explode(col("body_text")).alias("paragraph")) \
        .select(col("paragraph.text").alias("text")) \
        .withColumn("strains", extract_strains_udf(col("text"))) \
        .select(explode(col("strains")).alias("strain_data")) \
        .select(
            col("strain_data.category").alias("category"),
            col("strain_data.strain").alias("strain")
        )

    # Zapisanie wyników
    strains_df.write.csv(output_strains_path, header=True, mode="overwrite")

    spark.stop()
