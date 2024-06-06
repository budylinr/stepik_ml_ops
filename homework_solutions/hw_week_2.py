# настроить токены для почты https://stepik.org/lesson/1074008/step/13?unit=1084080
# сразу можно настроить S3 https://stepik.org/lesson/1074010/step/3?unit=1084082
# добавить токены в .env по примеру .env_example
# docker-compose up -d --build
# в airflow в Admin - Connections руками добавить s3_connection и pg_connection
# настройки s3_connection
# {
#     "connection Id": "s3_connection",
#     "connection Type": "Amazon Web Services",
#     "AWS Access Key ID": AWS_ACCESS_KEY_ID,
#     "AWS Secret Access Key": AWS_SECRET_ACCESS_KEY,
#     "Extra": {
#           "endpoint_url": "https://storage.yandexcloud.net",
#           "region_name": "ru-centrali"
#     }
# }
# настройки pg_connection
# {
#     "connection Id": "pg_connection",
#     "connection Type": "Postgres",
#     "Host": "postgres",
#     "Database": "postgres",
#     "Login": "airflow",
#     "Password": "airflow",
#     "Extra": {}
# }
