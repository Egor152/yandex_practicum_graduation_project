import logging
import vertica_python
from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.vertica.operators.vertica import VerticaOperator
from airflow.contrib.hooks.vertica_hook import VerticaHook
log = logging.getLogger(__name__)

vertica_hook = VerticaHook(vertica_conn_id='vertica_conn_id')

vertica_conn = vertica_hook.get_conn()




# Здесь я решил не переделывать SQL-запросы, потому что здесь использую цикл для вставки данных и запросы короткие
user = 'Скрыл по просьбе Яндекс Практикума'
password = 'Скрыл по просьбе Яндекс Практикума'
vertica_conn_info = {
    'host': 'Скрыл по просьбе Яндекс Практикума',
    'port': 'Скрыл по просьбе Яндекс Практикума',
    'user': user,
    'password': password
}

query_for_currencies = r"""TRUNCATE TABLE STV2024111513__STAGING.currencies;
                           COPY STV2024111513__STAGING.currencies(currency_code, currency_code_with, date_update, currency_with_div)
                           FROM LOCAL '/lessons/dags/data/currencies_history.csv'
                           DELIMITER ',';
                        """

query_for_clearing_transactions = 'TRUNCATE TABLE STV2024111513__STAGING.transactions'



def query_for_stg(query):
    with vertica_python.connect(**vertica_conn_info) as conn:
        cur = conn.cursor()
        cur.execute(query)
        conn.commit()
        




default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 12, 26)
}


with DAG(
        dag_id="filling_stg_and_inc",
        schedule_interval=None,
        default_args=default_args,
        catchup=False
) as dag:
    
    


    filling_currencies_task = PythonOperator(
                        task_id='filling_currencies_task',
                        python_callable=query_for_stg,
                        op_kwargs={'query':query_for_currencies},
                            )
    clearing_transactions_task = PythonOperator(
                                task_id='clearing_transactions_task',
                                python_callable=query_for_stg,
                                op_kwargs={'query':query_for_clearing_transactions}
                                     )

    filling_transaction_task = [
                                PythonOperator(
                        task_id=f'filling_transactions_batch_{i}',
                        python_callable=query_for_stg,
                        op_kwargs={'query':f"""
                              COPY STV2024111513__STAGING.transactions(operation_id, account_number_from, account_number_to,
                              currency_code, country,
                              status, transaction_type, amount, transaction_dt)
                              FROM LOCAL '/lessons/dags/data/transactions_batch_{i}.csv'
                              DELIMITER ',';"""},
                            ) for i in range(1,11)
                                ]

    filling_global_metrics_inc_task = VerticaOperator(
        task_id = 'filling_global_metrics_inc',
        vertica_conn_id = 'vertica_conn_id',
        sql = '/sql/filling_inc.sql',
        dag=dag
    )



    filling_currencies_task >> clearing_transactions_task >> filling_transaction_task >> filling_global_metrics_inc_task