from airflow.models import DAG
from airflow.utils.dates import days_ago
from datetime import timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.email_operator import EmailOperator
import logging


DEFAULT_ARGS = {
    "owner": "Roman Budylin",
    "email": "budylin@yandex.ru",
    "email_on_failure": True,
    "email_on_retry": True,
    "retry": 1,
    "retry_delay": timedelta(minutes=1),
}

dag = DAG(
    dag_id="homework_1",
    schedule_interval="0 * * * *",
    start_date=days_ago(2),
    catchup=False,
    tags=["ryabudylin"],
    default_args=DEFAULT_ARGS
)


def get_data_from_s3():
    s3_hook = S3Hook("s3_connection")
    file_path = s3_hook.download_file(
        key='iris_x.csv',
        bucket_name = 'ryabudylin-stepik-ml-ops',
    )
    print(file_path)
    print("first")


def second():
    print("second")


get_data_from_s3 = PythonOperator(
    task_id="first",
    python_callable=get_data_from_s3,
    dag=dag
)

second_task = PythonOperator(
    task_id='second',
    python_callable=second,
    dag=dag,
)

get_data_from_s3 >> second_task
