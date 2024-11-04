import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

#Variables
metadata_path = "/data/archive/metadata.csv"
json_folder_path = "/data/archive/noncomm_use_subset/noncomm_use_subset/pdf_json"
output_metadata_path = "/data/bronze/metadata.csv"
partition_number = 3


dag = DAG(
    dag_id = "Covid-19-pipeline",
    default_args = {
        "owner": "Anna and Mikolaj",
        "start_date": airflow.utils.dates.days_ago(1)
    },
    schedule_interval = "@daily"
)

start = PythonOperator(
    task_id="start",
    python_callable = lambda: print("Jobs started"),
    dag=dag
)

source_data_job = SparkSubmitOperator(
    task_id="source_data_job",
    conn_id="spark-conn",
    application="jobs/python/load_data.py",
    application_args=[metadata_path, json_folder_path, output_metadata_path, str(partition_number)],
    dag=dag
)

health_check_job = SparkSubmitOperator(
    task_id="health_check_job",
    conn_id="spark-conn",
    application="jobs/python/wordcountjob.py",
    dag=dag
)

end = PythonOperator(
    task_id="end",
    python_callable = lambda: print("Jobs completed successfully"),
    dag=dag
)

start >> health_check_job >> source_data_job >> end
