import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

#Variables
metadata_path = "/data/archive/metadata.csv"
json_folder_path = "/data/archive/noncomm_use_subset/noncomm_use_subset/pdf_json"
output_metadata_path = "/data/bronze/metadata.csv"
output_json_path = "/data/bronze/json_data"
partition_metadata_path = "/data/partition/metadata.csv"
partition_number = 3


dag = DAG(
    dag_id = "Covid-19-pipeline",
    default_args = {
        "owner": "Anna and Mikolaj",
        "start_date": airflow.utils.dates.days_ago(1)
    },
    schedule_interval = "@daily"
)

# Start task
start = PythonOperator(
    task_id="start",
    python_callable = lambda: print("Jobs started"),
    dag=dag
)

# Step 1: Health check
health_check_job = SparkSubmitOperator(
    task_id="health_check_job",
    conn_id="spark-conn",
    application="jobs/python/wordcountjob.py",
    dag=dag
)

# Step 2 1: Source data job
load_metadata_job = SparkSubmitOperator(
    task_id="load_metadata",
    conn_id="spark-conn",
    application="jobs/python/load_metadata.py",
    application_args=[metadata_path, output_metadata_path, str(partition_number)],
    dag=dag
)

# Step 2 2: Source data job
load_jsons_job = SparkSubmitOperator(
    task_id="load_jsons",
    conn_id="spark-conn",
    application="jobs/python/load_json.py",
    application_args=[json_folder_path, output_json_path, str(partition_number)],
    dag=dag
)


load_to_silver_job = PythonOperator(
    task_id="load_to_silver",
    python_callable = lambda: print("Load to silver started"),
    dag=dag
)

end = PythonOperator(
    task_id="end",
    python_callable = lambda: print("Jobs completed successfully"),
    dag=dag
)

start >> health_check_job >> [load_metadata_job, load_jsons_job] >> load_to_silver_job >> end
