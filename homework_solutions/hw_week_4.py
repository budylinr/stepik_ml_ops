import json
import logging
from typing import Literal, Dict, Any

import numpy as np
import os
import pandas as pd
import pickle

from datetime import datetime, timedelta
from sklearn.ensemble import RandomForestRegressor, HistGradientBoostingRegressor
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, median_absolute_error, r2_score
from sklearn.preprocessing import StandardScaler

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago


OWNER = os.getenv('OWNER')
EMAIL = os.getenv('EMAIL')
BUCKET = os.getenv('BUCKET')


DEFAULT_ARGS = {
    "owner": OWNER,
    "email": EMAIL,
    "email_on_failure": False,
    "email_on_retry": False,
    "retry": 1,
    "retry_delay": timedelta(minutes=1),
}


logger = logging.getLogger()
logger.addHandler(logging.StreamHandler())


DATA_PATH = 'datasets/california_housing.pkl' # TO-DO: your path
FEATURES = ["MedInc", "HouseAge", "AveRooms", "AveBedrms",
            "Population", "AveOccup", "Latitude", "Longitude"]

TARGET = "MedHouseVal"


models = dict(zip(
    ["rf", "lr", "hgb"],
    [RandomForestRegressor(), LinearRegression(), HistGradientBoostingRegressor()]
))


def create_dag(dag_id: str, model_name: Literal["rf", "lr", "hgb"]):
    def init() -> Dict[str, Any]:
        metrics = {}
        metrics['model'] = model_name
        metrics['start_timestamp'] = datetime.now().strftime('%Y%m%d %H:%M')
        logger.info("Train pipeline started.")
        return metrics

    def get_data_from_postgres(**kwargs) -> Dict[str, Any]:
        ti = kwargs['ti']
        metrics = ti.xcom_pull(task_ids='init')
        logger.info(f'metrics={metrics}')
        return metrics

    def prepare_data(**kwargs) -> Dict[str, Any]:
        ti = kwargs['ti']
        metrics = ti.xcom_pull(task_ids='get_data')
        return metrics

    def train_model(**kwargs) -> Dict[str, Any]:
        ti = kwargs['ti']
        metrics = ti.xcom_pull(task_ids='prepare_data')

        # TO-DO: Заполнить все шаги
        # Использовать созданный ранее S3 connection
        s3_hook = S3Hook('s3_connection')
        # Загрузить готовые данные с S3
        data = {}
        for name in ['X_train', 'X_test', 'y_train', 'y_test']:
            pickle_file = s3_hook.download_file(key=f'datasets/{name}.pkl', bucket_name=BUCKET)
            data[name] = pd.read_pickle(pickle_file)
            # without this fix i get error 'cannot set WRITEABLE flag to True of this array'
            # during model.fit
            # data[name].flags doesn't have flag WRITEABLE
            if 'y' in name:
                data[name] = np.array(data[name])
            print(type(data[name]))
            print(data[name].shape)
        logger.info('download files from s3')

        # Обучить модель
        model = models[model_name]
        metrics['train_start'] = datetime.now().strftime('%Y%m%d %H:%M')
        model.fit(data['X_train'], data['y_train'])
        metrics['train_end'] = datetime.now().strftime('%Y%m%d %H:%M')
        # Посчитать метрики
        prediction = model.predict(data['X_test'])
        y_test = data['y_test']
        metrics['r2_score'] = r2_score(y_test, prediction)
        metrics['mse'] = mean_squared_error(y_test, prediction)
        metrics['mae'] = median_absolute_error(y_test, prediction)
        return metrics

    def save_results(**kwargs) -> None:
        ti = kwargs['ti']
        metrics = ti.xcom_pull(task_ids='train_model')
        metrics['pipeline_end_timestamp '] = datetime.now().strftime('%Y%m%d %H:%M')
        s3_hook = S3Hook('s3_connection')
        # Сохранить файл в формате pkl на S3
        date = datetime.now().strftime('%Y_%m_%d_%H')
        data_path = f'results/{model_name}_{date}_metrics.json'
        session = s3_hook.get_session('ru-centrali')
        object_to_save = json.dumps(metrics)
        resource = session.resource('s3')
        resource.Object(BUCKET, data_path).put(Body=object_to_save)

    dag = DAG(
        dag_id=dag_id,
        schedule_interval="0 * * * *",
        start_date=days_ago(2),
        catchup=False,
        tags=["custom"],
        default_args=DEFAULT_ARGS
    )

    with dag:
        task_init = PythonOperator(
            task_id='init',
            python_callable=init,
            dag=dag,
        )
        task_get_data = PythonOperator(
            task_id='get_data',
            python_callable=get_data_from_postgres,
            dag=dag,
            provide_context=True,
        )
        task_prepare_data = PythonOperator(
            task_id='prepare_data',
            python_callable=prepare_data,
            dag=dag,
            provide_context=True,
        )
        task_train_model = PythonOperator(
            task_id='train_model',
            python_callable=train_model,
            dag=dag,
            provide_context=True,
        )
        task_save_results = PythonOperator(
            task_id='save_results',
            python_callable=save_results,
            dag=dag,
            provide_context=True,
        )

        # TO-DO: Архитектура DAG'а
        task_init >> task_get_data >> task_prepare_data >> task_train_model >> task_save_results


for model_name in models:
     create_dag(f'{model_name}_train', model_name)


@dag(
    schedule_interval="0 * * * *",
    start_date=days_ago(2),
    catchup=False,
    tags=["custom"],
    default_args=DEFAULT_ARGS,
)
def parallel_train_model():
    @task
    def init() -> Dict[str, Any]:
        metrics = {}
        metrics['start_timestamp'] = datetime.now().strftime('%Y%m%d %H:%M')
        logger.info("Train pipeline started.")
        return metrics

    @task
    def get_data_from_postgres(**kwargs) -> Dict[str, Any]:
        ti = kwargs['ti']
        metrics = ti.xcom_pull(task_ids='init')
        logger.info(f'metrics={metrics}')
        return metrics

    @task
    def prepare_data(**kwargs) -> Dict[str, Any]:
        ti = kwargs['ti']
        metrics = ti.xcom_pull(task_ids='get_data_from_postgres')
        return metrics

    @task
    def train_model(model_name, **kwargs) -> Dict[str, Any]:
        ti = kwargs['ti']
        metrics = ti.xcom_pull(task_ids='prepare_data')

        # TO-DO: Заполнить все шаги
        # Использовать созданный ранее S3 connection
        s3_hook = S3Hook('s3_connection')
        # Загрузить готовые данные с S3
        data = {}
        for name in ['X_train', 'X_test', 'y_train', 'y_test']:
            pickle_file = s3_hook.download_file(key=f'datasets/{name}.pkl', bucket_name=BUCKET)
            data[name] = pd.read_pickle(pickle_file)
            # without this fix i get error 'cannot set WRITEABLE flag to True of this array'
            # during model.fit
            # data[name].flags doesn't have flag WRITEABLE
            if 'y' in name:
                data[name] = np.array(data[name])
            print(type(data[name]))
            print(data[name].shape)
        logger.info('download files from s3')

        # Обучить модель
        model = models[model_name]
        metrics['model'] = model_name
        metrics['train_start'] = datetime.now().strftime('%Y%m%d %H:%M')
        model.fit(data['X_train'], data['y_train'])
        metrics['train_end'] = datetime.now().strftime('%Y%m%d %H:%M')
        # Посчитать метрики
        prediction = model.predict(data['X_test'])
        y_test = data['y_test']
        metrics['r2_score'] = r2_score(y_test, prediction)
        metrics['mse'] = mean_squared_error(y_test, prediction)
        metrics['mae'] = median_absolute_error(y_test, prediction)
        return metrics

    @task
    def save_results(train_model_task_ids, **kwargs) -> None:
        ti = kwargs['ti']
        metrics_list = ti.xcom_pull(task_ids=train_model_task_ids)
        for metrics in metrics_list:
            model_name = metrics['model']
            metrics['pipeline_end_timestamp '] = datetime.now().strftime('%Y%m%d %H:%M')

            s3_hook = S3Hook('s3_connection')
            # Сохранить файл в формате pkl на S3
            date = datetime.now().strftime('%Y_%m_%d_%H')
            data_path = f'hw_w4_parallel/{model_name}_{date}_metrics.json'
            session = s3_hook.get_session('ru-centrali')
            object_to_save = json.dumps(metrics)
            resource = session.resource('s3')
            resource.Object(BUCKET, data_path).put(Body=object_to_save)

    init_task = init()
    get_data_from_postgres_task = get_data_from_postgres()
    prepare_data_task = prepare_data()

    train_model_tasks = []
    for model_name in models:
        train_model_tasks.append(
            train_model(model_name)
        )

    train_model_task_ids = [item.operator.task_id for item in train_model_tasks]

    save_results_task = save_results(train_model_task_ids)

    init_task >> get_data_from_postgres_task >> prepare_data_task >> train_model_tasks >> save_results_task


parallel_train_model()
