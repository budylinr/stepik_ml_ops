import io
import json
import logging
import numpy as np
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

DEFAULT_ARGS = {
    # TO-DO: прописать аргументы
}

dag = DAG(  # TO-DO: прописать аргументы
)

_LOG=logging.getLogger()
_LOG.addHandler(logging.StreamHandler())

BUCKET =  # TO-DO: your bucket
DATA_PATH =  # TO-DO: your path
FEATURES = ["MedInc", "HouseAge", "AveRooms", "AveBedrms",
            "Population", "AveOccup", "Latitude", "Longitude"]
TARGET = "MedHouseVal"


def init() -> None:
    _LOG.info("Train pipeline started.")


def get_data_from_postgres() -> None:


# TO-DO: Заполнить все шаги

# Использовать созданный ранее PG connection

# Прочитать все данные из таблицы california_housing

# Использовать созданный ранее S3 connection

# Сохранить файл в формате pkl на S3


def prepare_data() -> None:


# TO-DO: Заполнить все шаги

# Использовать созданный ранее S3 connection

# Сделать препроцессинг
# Разделить на фичи и таргет

# Разделить данные на обучение и тест

# Обучить стандартизатор на train

# Сохранить готовые данные на S3

def train_model() -> None:


# TO-DO: Заполнить все шаги

# Использовать созданный ранее S3 connection

# Загрузить готовые данные с S3

# Обучить модель

# Посчитать метрики

# Сохранить результат на S3


def save_results() -> None:
    _LOG.info("Success.")


task_init =  # TO-DO: написать оператор

task_get_data =  # TO-DO: написать оператор

task_prepare_data =  # TO-DO: написать оператор

task_train_model =  # TO-DO: написать оператор

task_save_results =  # TO-DO: написать оператор

# TO-DO: Архитектура DAG'а
