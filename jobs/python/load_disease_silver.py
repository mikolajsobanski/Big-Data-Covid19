from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, udf
from pyspark.sql.types import ArrayType, StructType, StructField, StringType

# Lista chorób i ich kategorie
disease_categories = [
    {"disease": "COVID-19", "category": "Respiratory Diseases"},
    {"disease": "SARS", "category": "Respiratory Diseases"},
    {"disease": "Influenza", "category": "Respiratory Diseases"},
    {"disease": "Ebola", "category": "Infectious Diseases"},
    {"disease": "H1N1", "category": "Respiratory Diseases"},
    {"disease": "MERS", "category": "Respiratory Diseases"},
    {"disease": "Tuberculosis", "category": "Infectious Diseases"},
    {"disease": "Malaria", "category": "Infectious Diseases"},
    {"disease": "HIV", "category": "Infectious Diseases"},
    {"disease": "Pneumonia", "category": "Respiratory Diseases"},
    {"disease": "Chronic Obstructive Pulmonary Disease", "category": "Respiratory Diseases"},
    {"disease": "Asthma", "category": "Respiratory Diseases"},
    {"disease": "Acute Respiratory Distress Syndrome", "category": "Respiratory Diseases"},
    {"disease": "Respiratory failure", "category": "Respiratory Diseases"},
    {"disease": "Hypertension", "category": "Cardiovascular Diseases"},
    {"disease": "Heart failure", "category": "Cardiovascular Diseases"},
    {"disease": "Myocardial infarction", "category": "Cardiovascular Diseases"},
    {"disease": "Coronary artery disease", "category": "Cardiovascular Diseases"},
    {"disease": "Arrhythmia", "category": "Cardiovascular Diseases"},
    {"disease": "Cardiomyopathy", "category": "Cardiovascular Diseases"},
    {"disease": "Diabetes", "category": "Metabolic Diseases"},
    {"disease": "Hyperglycemia", "category": "Metabolic Diseases"},
    {"disease": "Insulin resistance", "category": "Metabolic Diseases"},
    {"disease": "Obesity", "category": "Metabolic Diseases"},
    {"disease": "Metabolic syndrome", "category": "Metabolic Diseases"},
    {"disease": "Chronic Kidney Disease", "category": "Kidney Diseases"},
    {"disease": "Acute Kidney Injury", "category": "Kidney Diseases"},
    {"disease": "Dialysis", "category": "Kidney Diseases"},
    {"disease": "End-stage renal disease", "category": "Kidney Diseases"},
    {"disease": "Cancer", "category": "Cancers"},
    {"disease": "Chemotherapy", "category": "Cancers"},
    {"disease": "Immunotherapy", "category": "Cancers"},
    {"disease": "Lung cancer", "category": "Cancers"},
    {"disease": "Hematological malignancies", "category": "Cancers"},
    {"disease": "Solid tumors", "category": "Cancers"},
    {"disease": "Rheumatoid arthritis", "category": "Autoimmune Diseases"},
    {"disease": "Systemic lupus erythematosus", "category": "Autoimmune Diseases"},
    {"disease": "Multiple sclerosis", "category": "Autoimmune Diseases"},
    {"disease": "Psoriasis", "category": "Autoimmune Diseases"},
    {"disease": "Crohn's disease", "category": "Autoimmune Diseases"},
    {"disease": "Immunosuppression", "category": "Autoimmune Diseases"},
    {"disease": "Alzheimer's disease", "category": "Neurological Diseases"},
    {"disease": "Parkinson's disease", "category": "Neurological Diseases"},
    {"disease": "Stroke", "category": "Neurological Diseases"},
    {"disease": "Depression", "category": "Psychiatric Disorders"},
    {"disease": "Anxiety", "category": "Psychiatric Disorders"},
    {"disease": "Post-traumatic stress disorder", "category": "Psychiatric Disorders"},
    {"disease": "Liver cirrhosis", "category": "Liver Diseases"},
    {"disease": "Hepatitis", "category": "Liver Diseases"},
    {"disease": "Fatty liver disease", "category": "Liver Diseases"},
    {"disease": "Hepatic failure", "category": "Liver Diseases"},
    {"disease": "Pneumocystis pneumonia", "category": "Infectious Diseases"},
    {"disease": "Cytomegalovirus", "category": "Infectious Diseases"},
    {"disease": "Fungal infections", "category": "Infectious Diseases"},
    {"disease": "Pregnancy complications", "category": "Other Diseases"},
    {"disease": "Thrombosis", "category": "Other Diseases"},
    {"disease": "Coagulopathy", "category": "Other Diseases"},
    {"disease": "Sepsis", "category": "Other Diseases"},
    {"disease": "Multi-organ failure", "category": "Other Diseases"}
]

# Funkcja do ekstrakcji chorób i przypisywania kategorii
def extract_diseases_and_categories(text):
    result = []
    for entry in disease_categories:
        if entry["disease"].lower() in text.lower():
            result.append({"disease": entry["disease"], "category": entry["category"]})
    return result

if __name__ == "__main__":
    # Argumenty wejściowe
    import sys
    json_folder_path = sys.argv[1]
    output_disease_path = sys.argv[2]

    # Inicjalizacja sesji Spark
    spark = SparkSession.builder \
        .appName("Extract Diseases with Categories") \
        .getOrCreate()

    # Wczytanie danych JSON
    json_df = spark.read.option("multiline", "true").json(json_folder_path)

    # Zdefiniowanie schematu dla UDF
    schema = ArrayType(
        StructType([
            StructField("disease", StringType(), True),
            StructField("category", StringType(), True)
        ])
    )

    # UDF dla Spark
    extract_diseases_and_categories_udf = udf(extract_diseases_and_categories, schema)

    # Przetwarzanie danych
    diseases_df = json_df.select(explode(col("body_text")).alias("paragraph")) \
        .select(col("paragraph.text").alias("text")) \
        .withColumn("diseases_with_categories", extract_diseases_and_categories_udf(col("text"))) \
        .select(explode(col("diseases_with_categories")).alias("disease_category")) \
        .select(col("disease_category.disease").alias("disease"), col("disease_category.category").alias("category")) \
       # .distinct()

    # Zapisanie wyników
    diseases_df.write.csv(output_disease_path, header=True, mode="overwrite")

    spark.stop()