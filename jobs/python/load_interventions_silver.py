from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, udf
from pyspark.sql.types import ArrayType, StructType, StructField, StringType

# Lista interwencji zdrowotnych z kategoriami i podkategoriami
interventions = [
    {"category": "Pharmaceutical", "subcategory": "Vaccines", "keywords": ["vaccine", "vaccination", "mRNA vaccine", "Pfizer", "Moderna", "AstraZeneca", "Johnson & Johnson"]},
    {"category": "Pharmaceutical", "subcategory": "Antiviral Drugs", "keywords": ["remdesivir", "molnupiravir", "favipiravir", "antiviral therapy"]},
    {"category": "Pharmaceutical", "subcategory": "Corticosteroids", "keywords": ["dexamethasone", "steroids"]},
    {"category": "Pharmaceutical", "subcategory": "Immunomodulatory Drugs", "keywords": ["tocilizumab", "baricitinib", "monoclonal antibodies"]},
    {"category": "Non-Pharmaceutical", "subcategory": "Lockdowns", "keywords": ["lockdown", "quarantine", "stay-at-home order", "shelter-in-place"]},
    {"category": "Non-Pharmaceutical", "subcategory": "Social Distancing", "keywords": ["social distancing", "physical distancing"]},
    {"category": "Non-Pharmaceutical", "subcategory": "Mask Wearing", "keywords": ["mask mandate", "face masks", "respiratory protection"]},
    {"category": "Non-Pharmaceutical", "subcategory": "Hygiene", "keywords": ["hand washing", "sanitizers", "hand hygiene"]},
    {"category": "Non-Pharmaceutical", "subcategory": "Testing and Contact Tracing", "keywords": ["contact tracing", "testing", "PCR tests", "antigen tests"]},
    {"category": "Non-Pharmaceutical", "subcategory": "Isolation and Quarantine", "keywords": ["isolation", "quarantine", "self-isolation", "home isolation"]},
    {"category": "Non-Pharmaceutical", "subcategory": "Healthcare and Hospitalization", "keywords": ["ICU care", "ventilation", "oxygen therapy", "critical care"]}
]

# Funkcja do ekstrakcji interwencji zdrowotnych
def extract_interventions(text):
    result = []
    for intervention in interventions:
        for keyword in intervention["keywords"]:
            if keyword.lower() in text.lower():
                result.append({
                    "category": intervention["category"],
                    "subcategory": intervention["subcategory"],
                    "keyword": keyword
                })
    return result

if __name__ == "__main__":
    import sys
    input_json_path = sys.argv[1]
    output_interventions_path = sys.argv[2]

    spark = SparkSession.builder.appName("Extract Health Interventions").getOrCreate()

    # Wczytanie danych JSON
    json_df = spark.read.option("multiline", "true").json(input_json_path)

    # Schemat dla UDF
    schema = ArrayType(
        StructType([
            StructField("category", StringType(), True),
            StructField("subcategory", StringType(), True),
            StructField("keyword", StringType(), True)
        ])
    )

    # UDF dla Spark
    extract_interventions_udf = udf(extract_interventions, schema)

    # Ekstrakcja interwencji zdrowotnych
    interventions_df = json_df.select(explode(col("body_text")).alias("paragraph")) \
        .select(col("paragraph.text").alias("text")) \
        .withColumn("interventions", extract_interventions_udf(col("text"))) \
        .select(explode(col("interventions")).alias("intervention")) \
        .select(
            col("intervention.category").alias("category"),
            col("intervention.subcategory").alias("subcategory"),
            col("intervention.keyword").alias("keyword")
        )

    # Zapisanie wyników
    interventions_df.write.csv(output_interventions_path, header=True, mode="overwrite")

    spark.stop()
