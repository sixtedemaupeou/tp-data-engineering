from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

from tasks import fetch_filter_store_datasets

with DAG(
    dag_id='dag_tp',
    schedule_interval='0 17 * * WED',
    start_date=days_ago(1),
    dagrun_timeout=timedelta(minutes=60),
    tags=['example', 'python', 'bash'],
    params={"example_key": "example_value"},
) as dag:
    dag.doc_md = __doc__
    dag.doc_md = """
    This is a DAG built for the data engineering TP
    """

    fetch_task = PythonOperator(
        task_id="fetch-datasets", 
        python_callable=fetch_filter_store_datasets
    )

    test_bash = BashOperator(
        task_id='test-bash',
        bash_command='echo 1',
    )


    fetch_task >> test_bash

