from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import subprocess

from tasks import extract_top_datasets, fetch_filter_store_datasets

minio_container_ip = '172.30.0.3'# subprocess.run(["docker inspect -f '{{range.NetworkSettings.Networks}}{{.IPAddress}}{{end}}' minio"], stdout=subprocess.PIPE).stdout
mongo_container_ip = '172.30.0.4'# subprocess.run(["docker inspect -f '{{range.NetworkSettings.Networks}}{{.IPAddress}}{{end}}' mongo"], stdout=subprocess.PIPE).stdout

with DAG(
    dag_id='dag_tp',
    schedule_interval='0 17 * * WED',
    start_date=days_ago(1),
    dagrun_timeout=timedelta(minutes=60),
    tags=['python', 'minio', 'mongo'],
) as dag:
    dag.doc_md = __doc__
    dag.doc_md = """
    This is a DAG built for the data engineering TP
    """
    data_gouv_to_s3_task = PythonOperator(
        task_id="datasets-to-s3", 
        python_callable=fetch_filter_store_datasets,
        op_kwargs={"minio_container_ip": minio_container_ip},
    )

    s3_to_mongo_task = PythonOperator(
        task_id='dataset-to-mongo',
        python_callable=extract_top_datasets,
        op_kwargs={"minio_container_ip": minio_container_ip, "mongo_container_ip": mongo_container_ip},
    )


    data_gouv_to_s3_task >> s3_to_mongo_task

