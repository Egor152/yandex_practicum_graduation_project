import boto3
import pendulum
import logging
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.decorators import dag
from logging import Logger


log = logging.getLogger(__name__)

# Хотел сделать подключение в Aifrlow, чтобы в коде не писать кредиты, но через Airflow ловлю ошибку, что токен устарел
AWS_ACCESS_KEY_ID = 'Скрыл по просьбе Яндекс Практикума'
AWS_SECRET_ACCESS_KEY = 'Скрыл по просьбе Яндекс Практикума'
bucket = 'final-project'

def fetch_s3_file(bucket: str, key: str) -> str:
    session = boto3.session.Session()
    s3_client = session.client(
        service_name='s3',
        endpoint_url='Скрыл по просьбе Яндекс Практикума',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )

    log.info('Подключились к хранилищу')
    filename = f'/data/{key}'
    s3_client.download_file(
				Bucket=bucket, 
				Key=key, 
				Filename=filename
        )
    log.info(f'Логи после изменения пути записи')
    log.info(f'Скачали данные из Bucket={bucket}, key = {key}, filename = {filename}')
    
bash_command_tmpl = "head {{ params.files }}"

@dag(schedule_interval=None, start_date=pendulum.parse('2024-12-23'))

def get_data_from_s3_after_reworking():
    
    # задача для скачивания файла о курсах валют
    currencies_history_task = PythonOperator(
            task_id=f'fetch_for_currencies_history', 
            python_callable=fetch_s3_file,
            op_kwargs={'bucket': bucket, 'key': 'currencies_history.csv'},
        )

    # список задач для скачивания данных о транзакциях
    transactions_tasks = [
        PythonOperator(
            task_id=f'fetch_for_transactions_{i}batch', 
            python_callable=fetch_s3_file,
            op_kwargs={'bucket': bucket, 'key': f'transactions_batch_{i}.csv'},
        ) for i in range(1,11)
                ]

    currencies_history_task >> transactions_tasks 
 
 
_ = get_data_from_s3_after_reworking()

 
 