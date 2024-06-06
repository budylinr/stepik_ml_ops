# import io
# import json
import logging
import numpy as np
import os
import pandas as pd
import pickle

from datetime import datetime, timedelta
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, median_absolute_error, r2_score
from sklearn.preprocessing import StandardScaler

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
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

dag = DAG(
    dag_id="hw_week_3",
    schedule_interval="0 * * * *",
    start_date=days_ago(2),
    catchup=False,
    tags=["custom"],
    default_args=DEFAULT_ARGS
)


logger = logging.getLogger()
logger.addHandler(logging.StreamHandler())


DATA_PATH = 'datasets/california_housing.pkl' # TO-DO: your path
FEATURES = ["MedInc", "HouseAge", "AveRooms", "AveBedrms",
            "Population", "AveOccup", "Latitude", "Longitude"]

TARGET = "MedHouseVal"


def save_to_s3(s3_hook: S3Hook, bucket: str, data_path: str, data) -> None:
    session = s3_hook.get_session('ru-centrali')
    object_to_save = pickle.dumps(data)
    resource = session.resource('s3')
    resource.Object(bucket, data_path).put(Body=object_to_save)


def init() -> None:
    logger.info("Train pipeline started.")


def get_data_from_postgres() -> None:
    # TO-DO: Заполнить все шаги
    # Использовать созданный ранее PG connection
    pg_hook = PostgresHook('pg_connection')
    con = pg_hook.get_conn()
    # Прочитать все данные из таблицы california_housing
    data = pd.read_sql_query('select * from california_housing', con)
    # Использовать созданный ранее S3 connection
    s3_hook = S3Hook('s3_connection')
    # Сохранить файл в формате pkl на S3
    save_to_s3(s3_hook, BUCKET, DATA_PATH, data)
    logger.info('data upload to S3 completed')


def prepare_data() -> None:
    # TO-DO: Заполнить все шаги
    # Использовать созданный ранее S3 connection
    s3_hook = S3Hook('s3_connection')
    pickle_file = s3_hook.download_file(key=DATA_PATH, bucket_name=BUCKET)
    logger.info('download file from s3')
    data = pd.read_pickle(pickle_file)
    logger.info('load data')
    # Сделать препроцессинг
    # Разделить на фичи и таргет
    X, y = data[FEATURES], data[TARGET]
    # Разделить данные на обучение и тест
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=17)
    # Обучить стандартизатор на train
    scaler = StandardScaler()
    X_train_fitted = scaler.fit_transform(X_train)
    X_test_fitted = scaler.transform(X_test)

    # Сохранить готовые данные на S3
    for name, data in zip(
            ['X_train', 'X_test', 'y_train', 'y_test'],
            [X_train_fitted, X_test_fitted, y_train, y_test]
    ):
        save_to_s3(s3_hook, BUCKET, f'datasets/{name}.pkl', data)


def train_model() -> None:
    # TO-DO: Заполнить все шаги
    # Использовать созданный ранее S3 connection
    s3_hook = S3Hook('s3_connection')
    # Загрузить готовые данные с S3
    res_list = []
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
    model = RandomForestRegressor()
    model.fit(data['X_train'], data['y_train'])
    # Посчитать метрики
    prediction = model.predict(data['X_test'])
    result = {}
    y_test = data['y_test']
    result['r2_score'] = r2_score(y_test, prediction)
    result['mse'] = mean_squared_error(y_test, prediction)
    result['mae'] = median_absolute_error(y_test, prediction)
    # Сохранить результат на S3
    save_to_s3(s3_hook,BUCKET, 'datasets/metrics.pkl', result)


def save_results() -> None:
    logger.info("Success.")


# TO-DO: написать оператор
task_init = PythonOperator(
    task_id='init',
    python_callable=init,
    dag=dag,
)

# TO-DO: написать оператор
task_get_data = PythonOperator(
    task_id='get_data',
    python_callable=get_data_from_postgres,
    dag=dag,
)
task_prepare_data = PythonOperator(
    task_id='prepare_data',
    python_callable=prepare_data,
    dag=dag,
)
task_train_model = PythonOperator(
    task_id='train_model',
    python_callable=train_model,
    dag=dag,
)
task_save_results = PythonOperator(
    task_id='save_results',
    python_callable = save_results,
    dag=dag,
)

# TO-DO: Архитектура DAG'а
task_init >> task_get_data >> task_prepare_data >> task_train_model >> task_save_results
