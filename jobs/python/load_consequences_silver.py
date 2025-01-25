from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, udf
from pyspark.sql.types import ArrayType, StructType, StructField, StringType

# Kategorie skutków COVID-19
categories = {
    "Respiratory Problems": [
        "ARDS", "acute respiratory distress syndrome", "dyspnea", "hypoxia",
        "cough", "pneumonia", "pulmonary fibrosis"
    ],
    "Cardiological Problems": [
        "myocarditis", "arrhythmias", "cardiomyopathy", "heart failure",
        "pulmonary embolism", "hypertension"
    ],
    "Neurological Problems": [
        "stroke", "encephalopathy", "neuropathy", "loss of smell", "anosmia",
        "loss of taste", "ageusia", "Guillain-Barré syndrome", "chronic headaches"
    ],
    "Psychological and Psychiatric Problems": [
        "anxiety", "depression", "PTSD", "Post-Traumatic Stress Disorder",
        "insomnia", "brain fog", "mental health"
    ],
    "Systemic Problems": [
        "fatigue", "long COVID", "cytokine storm", "cytokine storm syndrome",
        "sepsis", "immunosuppression"
    ],
    "Gastrointestinal Problems": [
        "diarrhea", "nausea", "vomiting", "abdominal pain", "loss of appetite"
    ],
    "Dermatological Problems": [
        "rashes", "COVID toes", "urticaria", "skin ulcers"
    ],
    "Metabolic and Endocrine Problems": [
        "COVID-induced diabetes", "hormonal imbalances", "glucose metabolism issues"
    ],
    "Hematological Problems": [
        "blood clots", "thrombocytopenia", "Disseminated Intravascular Coagulation", "DIC"
    ],
    "Urinary and Renal Problems": [
        "acute kidney injury", "urinary retention", "hematuria"
    ]
}

# Funkcja do ekstrakcji skutków COVID-19
def extract_effects(text):
    result = []
    for category, effects in categories.items():
        for effect in effects:
            if effect.lower() in text.lower():
                result.append({"category": category, "effect": effect})
    return result

if __name__ == "__main__":
    import sys
    input_json_path = sys.argv[1]
    output_effects_path = sys.argv[2]

    spark = SparkSession.builder.appName("Extract COVID Effects").getOrCreate()

    # Wczytanie danych JSON
    json_df = spark.read.option("multiline", "true").json(input_json_path)

    # Schemat dla UDF
    schema = ArrayType(
        StructType([
            StructField("category", StringType(), True),
            StructField("effect", StringType(), True)
        ])
    )

    # UDF dla Spark
    extract_effects_udf = udf(extract_effects, schema)

    # Ekstrakcja skutków COVID-19
    effects_df = json_df.select(explode(col("body_text")).alias("paragraph")) \
        .select(col("paragraph.text").alias("text")) \
        .withColumn("effects", extract_effects_udf(col("text"))) \
        .select(explode(col("effects")).alias("effect_data")) \
        .select(
            col("effect_data.category").alias("category"),
            col("effect_data.effect").alias("effect")
        )

    # Zapisanie wyników
    effects_df.write.csv(output_effects_path, header=True, mode="overwrite")

    spark.stop()
