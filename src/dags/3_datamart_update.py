import logging
import vertica_python
from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.vertica.operators.vertica import VerticaOperator
from airflow.contrib.hooks.vertica_hook import VerticaHook
from airflow.operators.dummy import DummyOperator
log = logging.getLogger(__name__)

# Переделал подключение с помощью VerticaHook и запрос с VerticaOperator чтобы не было в коде

vertica_hook = VerticaHook(vertica_conn_id='vertica_conn_id')

vertica_conn = vertica_hook.get_conn()

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 12, 26)
}


with DAG(
        dag_id="filling_global_metrics_dag",
        schedule_interval=None,
        default_args=default_args,
        catchup=False
) as dag:
    
    # Перенес SQL в отдельный файл и добавил задачки-заглушки, чтобы ДАГ был более аккуратным
    start_task = DummyOperator(
    task_id='start_task',
    dag=dag,
            )

    filling_global_metrics_task = VerticaOperator(
        task_id = 'filling_global_metrics_task',
        vertica_conn_id = 'vertica_conn_id',
        sql = '/sql/cdm_query.sql',
        dag=dag
    )

    end_task = DummyOperator(
    task_id='end_task',
    dag=dag,
                )

    start_task >> filling_global_metrics_task >> end_task