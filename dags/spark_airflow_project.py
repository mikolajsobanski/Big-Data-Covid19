import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.task_group import TaskGroup

# Variables
metadata_path = "/data/archive/metadata.csv"
json_folder_path = "/data/archive/noncomm_use_subset/noncomm_use_subset/pdf_json"
output_metadata_path = "/data/bronze/metadata.csv"
output_json_path = "/data/bronze/json_data"
output_author_path = "/data/silver/authors"
output_publication_path = "/data/silver/publications"
output_disease_path = "/data/silver/diseases"
output_interventions_path = "/data/silver/interventions"
output_coronavirus_path = "/data/silver/coronavirus"
output_consequences_path = "/data/silver/consequences"
output_analysis_path = "/data/gold/analysis"
partition_number = 3

dag = DAG(
    dag_id="Covid-19-pipeline",
    default_args={
        "owner": "Anna and Mikolaj",
        "start_date": airflow.utils.dates.days_ago(1),
    },
    schedule_interval="@daily",
)

# Start task
start = PythonOperator(
    task_id="start",
    python_callable=lambda: print("Jobs started"),
    dag=dag,
)

# Step 1: Health check
health_check_job = SparkSubmitOperator(
    task_id="health_check_job",
    conn_id="spark-conn",
    application="jobs/python/wordcountjob.py",
    dag=dag,
)

# Step 2: Source data jobs
load_metadata_job = SparkSubmitOperator(
    task_id="load_metadata",
    conn_id="spark-conn",
    application="jobs/python/load_metadata.py",
    application_args=[metadata_path, output_metadata_path, str(partition_number)],
    dag=dag,
)

load_jsons_job = SparkSubmitOperator(
    task_id="load_jsons",
    conn_id="spark-conn",
    application="jobs/python/load_json.py",
    application_args=[json_folder_path, output_json_path, str(partition_number)],
    dag=dag,
)

# Group: Load to Silver
with TaskGroup(group_id="load_to_silver_group", dag=dag) as load_to_silver_group:
    load_disease_to_silver_job = SparkSubmitOperator(
        task_id="load_disease_to_silver",
        conn_id="spark-conn",
        application="jobs/python/load_disease_silver.py",
        application_args=[json_folder_path, output_disease_path],
        dag=dag,
    )

    load_interventions_to_silver_job = SparkSubmitOperator(
        task_id="load_interventions_to_silver",
        conn_id="spark-conn",
        application="jobs/python/load_interventions_silver.py",
        application_args=[json_folder_path, output_interventions_path],
        dag=dag,
    )

    load_coronavirus_to_silver_job = SparkSubmitOperator(
        task_id="load_coronavirus_to_silver",
        conn_id="spark-conn",
        application="jobs/python/load_coronavirus_silver.py",
        application_args=[json_folder_path, output_coronavirus_path],
        dag=dag,
    )

    load_consequences_to_silver_job = SparkSubmitOperator(
        task_id="load_consequences_to_silver",
        conn_id="spark-conn",
        application="jobs/python/load_consequences_silver.py",
        application_args=[json_folder_path, output_consequences_path],
        dag=dag,
    )

    load_to_silver_job = SparkSubmitOperator(
        task_id="load_to_silver",
        conn_id="spark-conn",
        application="jobs/python/load_silver.py",
        application_args=[
            metadata_path,
            json_folder_path,
            output_author_path,
            output_publication_path,
            output_disease_path,
        ],
        dag=dag,
)



# Business questions
most_prolific_author_job = SparkSubmitOperator(
    task_id="most_prolific_author",
    conn_id="spark-conn",
    application="jobs/python/most_prolific_author.py",
    application_args=[
        output_author_path,
        output_publication_path,
        f"{output_analysis_path}/prolific_author",
    ],
    dag=dag,
)

most_common_disease_job = SparkSubmitOperator(
    task_id="most_common_disease",
    conn_id="spark-conn",
    application="jobs/python/most_common_disease.py",
    application_args=[
        output_disease_path,
        f"{output_analysis_path}/common_disease",
    ],
    dag=dag,
)

most_common_interventions_job = SparkSubmitOperator(
    task_id="most_common_interventions",
    conn_id="spark-conn",
    application="jobs/python/most_common_intervenctions.py",
    application_args=[
        output_interventions_path,
        f"{output_analysis_path}/common_interventions",
    ],
    dag=dag,
)

most_common_coronavirus_job = SparkSubmitOperator(
    task_id="most_common_coronavirus",
    conn_id="spark-conn",
    application="jobs/python/most_common_coronavirus.py",
    application_args=[
        output_coronavirus_path,
        f"{output_analysis_path}/common_coronavirus",
    ],
    dag=dag,
)

most_common_consequences_job = SparkSubmitOperator(
    task_id="most_common_consequences",
    conn_id="spark-conn",
    application="jobs/python/most_common_consequences.py",
    application_args=[
        output_consequences_path,
        f"{output_analysis_path}/common_consequences",
    ],
    dag=dag,
)

# End task
end = PythonOperator(
    task_id="end",
    python_callable=lambda: print("Jobs completed successfully"),
    dag=dag,
)

# DAG dependencies
start >> health_check_job >> [load_metadata_job, load_jsons_job] >> load_to_silver_group >> [
    most_prolific_author_job,
    most_common_disease_job,
    most_common_interventions_job,
    most_common_coronavirus_job,
    most_common_consequences_job,
] >> end
