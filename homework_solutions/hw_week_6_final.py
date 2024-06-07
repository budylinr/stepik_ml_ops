"""
  - BUCKET заменить на свой;
  - EXPERIMENT_NAME и DAG_ID оставить как есть (ссылками на переменную NAME);
  - имена коннекторов: pg_connection и s3_connection;
  - данные должны читаться из таблицы с названием california_housing;
  - данные на S3 должны лежать в папках {NAME}/datasets/ и {NAME}/results/.
"""

import json
import logging
import mlflow
import numpy as np
import os
import pandas as pd
import pickle

from datetime import datetime, timedelta
from sklearn.linear_model import LinearRegression
from sklearn.ensemble import RandomForestRegressor, HistGradientBoostingRegressor
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from typing import Any, Dict, Literal

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from mlflow.models import infer_signature


logger = logging.getLogger()
logger.addHandler(logging.StreamHandler())

NAME = os.getenv('TELEGRAM')  # TO-DO: Вписать свой ник в телеграме

BUCKET = os.getenv('BUCKET')  # TO-DO: Вписать свой бакет
FEATURES = [
    "MedInc", "HouseAge", "AveRooms", "AveBedrms", "Population", "AveOccup",
    "Latitude", "Longitude"
]
TARGET = "MedHouseVal"
EXPERIMENT_NAME = NAME
DAG_ID = NAME
EMAIL = os.getenv('EMAIL')


models = dict(zip(
    ["rf", "lr", "hgb"],
    [RandomForestRegressor(), LinearRegression(), HistGradientBoostingRegressor()]
))

default_args = {
    "owner": NAME,
    "email": EMAIL,
    "email_on_failure": False,
    "email_on_retry": False,
    "retry": 1,
    "retry_delay": timedelta(minutes=1),
}

dag = DAG(dag_id=DAG_ID,
          default_args=default_args,
          schedule_interval="0 * * * *",
          start_date=days_ago(2),
          catchup=False,
          tags=["custom", 'homework'],
)


def get_now_ts():
    return datetime.now().strftime('%Y%m%d %H:%M')


def save_pickle_to_s3(s3_hook: S3Hook, bucket: str, data_path: str, data) -> None:
    session = s3_hook.get_session('ru-centrali')
    object_to_save = pickle.dumps(data)
    resource = session.resource('s3')
    resource.Object(bucket, data_path).put(Body=object_to_save)


def init() -> Dict[str, Any]:
    logger.info("Train pipeline started.")

    # TO-DO 1 metrics: В этом шаге собрать start_tiemstamp, run_id, experiment_name, experiment_id.
    metrics = {}
    metrics['start_timestamp'] = get_now_ts()
    metrics['experiment_name'] = EXPERIMENT_NAME

    # TO-DO 2 mlflow: Создать эксперимент с experiment_name=NAME.
    # Добавить проверку на уже созданный эксперимент!
    # your code here.

    exp_name = NAME
    experiment = mlflow.get_experiment_by_name(exp_name)
    experiment_id = None
    if experiment:
        experiment_id = experiment.experiment_id
    if not experiment_id:
        experiment_id = mlflow.create_experiment(
            exp_name,
            artifact_location=f"s3://{os.getenv('BUCKET')}/{exp_name}",
        )
    mlflow.set_experiment(experiment_id=experiment_id)

    # TO-DO 3 mlflow: Создать parent run.
    # your code here.
    run_name = 'PARENT_RUN'
    with mlflow.start_run(
        # your code here.
        run_name=run_name,
    ) as parent_run:
        metrics['run_id'] = parent_run.info.run_id
        metrics['experiment_id'] = experiment_id
        return metrics


def get_data_from_postgres(**kwargs) -> Dict[str, Any]:
    # TO-DO 1 metrics: В этом шаге собрать data_download_start, data_download_end.
    # your code here.
    ti = kwargs['ti']
    metrics = ti.xcom_pull(task_ids='init')
    metrics['data_download_start'] = get_now_ts()

    # TO-DO 2 connections: Создать коннекторы.
    # your code here.
    pg_hook = PostgresHook('pg_connection')
    con = pg_hook.get_conn()
    s3_hook = S3Hook('s3_connection')

    logger.info('data upload to S3 completed')

    # TO-DO 3 Postgres: Прочитать данные.
    # your code here.
    data = pd.read_sql_query('select * from california_housing', con)

    # TO-DO 4 Postgres: Сохранить данные на S3 в формате pickle в папку {NAME}/datasets/.
    filename = f"{NAME}/datasets/california_housing.pkl"
    # your code here:
    save_pickle_to_s3(s3_hook, BUCKET, filename, data)

    metrics['data_download_end'] = get_now_ts()
    return metrics


def prepare_data(**kwargs) -> Dict[str, Any]:
    # TO-DO 1 metrics: В этом шаге собрать data_preparation_start, data_preparation_end.
    # your code here.
    ti = kwargs['ti']
    metrics = ti.xcom_pull(task_ids='get_data')
    metrics['data_preparation_start'] = get_now_ts()

    # TO-DO 2 connections: Создать коннекторы.
    # your code here.
    s3_hook = S3Hook('s3_connection')
    # TO-DO 3 S3: Прочитать данные с S3.
    filename = f"{NAME}/datasets/california_housing.pkl"
    # your code here.
    pickle_file = s3_hook.download_file(key=filename, bucket_name=BUCKET)
    logger.info('download file from s3')
    data = pd.read_pickle(pickle_file)
    logger.info('load data')

    # TO-DO 4 Сделать препроцессинг.
    # your code here.
    X, y = data[FEATURES], data[TARGET]

    # TO-DO 5 Разделить данные на train/test.
    # your code here.
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=17)

    # TO-DO 6 Подготовить 4 обработанных датасета.
    # your code here.
    scaler = StandardScaler()
    X_train_fitted = scaler.fit_transform(X_train)
    X_test_fitted = scaler.transform(X_test)

    # Сохранить данные на S3 в папку {NAME}/datasets/.
    for name, data in zip(
            ['X_train', 'X_test', 'y_train', 'y_test'],
            [X_train_fitted, X_test_fitted, y_train, y_test]
    ):
        filename = f'{NAME}/datasets/{name}.pkl'
        save_pickle_to_s3(s3_hook, BUCKET, filename, data)

    metrics['data_preparation_end'] = get_now_ts()
    return metrics


def train_mlflow_model(model: Any, name: str, X_train: np.array,
                       X_test: np.array, y_train: pd.Series,
                       y_test: pd.Series) -> None:

    # TO-DO 1: Обучить модель.
    # your code here
    model.fit(X_train, y_train)

    # TO-DO 2: Сделать predict.
    # your code here
    prediction = model.predict(X_test)

    # TO-DO 3: Сохранить результаты обучения с помощью MLFlow.
    # your code here
    signature = infer_signature(X_test, prediction)
    model_info = mlflow.sklearn.log_model(model, name, signature=signature)
    mlflow.evaluate(
        model_info.model_uri,
        data=X_test,
        targets=y_test,
        model_type="regressor",
        evaluators=["default"],
    )


def train_model(**kwargs) -> Dict[str, Any]:
    # TO-DO 1 metrics: В этом шаге собрать f"train_start_{model_name}" и f"train_end_{model_name}".
    # your code here.
    ti = kwargs['ti']
    metrics = ti.xcom_pull(task_ids='prepare_data')
    model_name = kwargs['model_name']
    metrics[f'train_start_{model_name}'] = get_now_ts()

    # TO-DO 2 connections: Создать коннекторы.
    # your code here.
    s3_hook = S3Hook('s3_connection')

    # TO-DO 3 S3: Прочитать данные с S3 из папки {NAME}/datasets/.
    # your code here.
    data = {}
    for name in ['X_train', 'X_test', 'y_train', 'y_test']:
        pickle_file = s3_hook.download_file(key=f'{NAME}/datasets/{name}.pkl', bucket_name=BUCKET)
        data[name] = pd.read_pickle(pickle_file)
        # without this fix i get error 'cannot set WRITEABLE flag to True of this array'
        # during model.fit
        # data[name].flags doesn't have flag WRITEABLE
        if 'y' in name:
            data[name] = np.array(data[name])

    # TO-DO 4: Обучить модели и залогировать обучение с помощью MLFlow.
    run_id = metrics['run_id']
    experiment_id = metrics['experiment_id']

    with mlflow.start_run(run_id=run_id) as parent_run:
        with mlflow.start_run(
                # your code here
            run_name=model_name,
            experiment_id=experiment_id,
            nested=True,
        ) as child_run:
            # your code here
            train_mlflow_model(
                models[model_name], model_name, data['X_train'], data['X_test'],
                data['y_train'], data['y_test'],
            )
    metrics[f'train_end_{model_name}'] = get_now_ts()
    return metrics


def save_results(**kwargs) -> None:
    # TO-DO 1 metrics: В этом шаге собрать end_timestamp.
    ti = kwargs['ti']
    metrics_list = ti.xcom_pull(task_ids=[
        f'train_model_{model_name}' for model_name in models
    ])
    metrics = {}
    for model_metrics in metrics_list:
        metrics.update(model_metrics)

    # TO-DO 2: сохранить результаты обучения на S3 в файл {NAME}/results/{date}.json.
    date = datetime.now().strftime('%Y_%m_%d_%H')
    filename = f"{NAME}/results/{date}.json"
    # your code here.
    s3_hook = S3Hook('s3_connection')
    session = s3_hook.get_session('ru-centrali')
    object_to_save = json.dumps(metrics)
    resource = session.resource('s3')
    resource.Object(BUCKET, filename).put(Body=object_to_save)


#################################### INIT DAG ####################################

# TO-DO: Прописать архитектуру DAG'a.

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
training_model_tasks = [
    PythonOperator(
        task_id=f'train_model_{model_name}',
        python_callable=train_model,
        dag=dag,
        provide_context=True,
        op_kwargs={'model_name': model_name},
    ) for model_name in models
]

task_save_results = PythonOperator(
    task_id='save_results',
    python_callable=save_results,
    dag=dag,
    provide_context=True,
)

# TO-DO: Архитектура DAG'а
task_init >> task_get_data >> task_prepare_data >> training_model_tasks >> task_save_results
