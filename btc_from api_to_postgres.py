
import logging as _log
import shutil
import sys
import tempfile
import time
from pprint import pprint
import json
import requests

import pendulum

from airflow import DAG, XComArg
# import airflow.models.xcom
from airflow.decorators import task
from airflow.operators.python import PythonOperator, ExternalPythonOperator, PythonVirtualenvOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator

log = _log.getLogger(__name__)

PATH_TO_PYTHON_BINARY = sys.executable

BASE_DIR = tempfile.gettempdir()

PG_CONN_ID='postgres_otus'
PG_TABLE='bitcoin'

URL_RESPONSE_BITСOIN=requests.get('https://api.coincap.io/v2/rates/bitcoin')

def get_bitcoin_rate():
        res_dict = URL_RESPONSE_BITСOIN.json()
        bitcoin_rate = res_dict.get('data').get('rateUsd')
        timestamp = res_dict.get('timestamp')
        print(bitcoin_rate,timestamp)

        print(json.dumps(res_dict, indent=4))
        _log.info("get bitcoin rate")
        return bitcoin_rate, timestamp

def put_to_psql(**context):
        sql_string = """ CREATE TABLE IF NOT EXISTS {0} (BITCOIN_RATE INT, TIMESTAMP BIGINT);
                         INSERT INTO {0} (BITCOIN_RATE, TIMESTAMP) VALUES ({1}, {2});
                     """ \
                     .format(PG_TABLE,get_bitcoin_rate()[0],get_bitcoin_rate()[1])

        print(sql_string)
        _log.info("create sql string")
        return sql_string


with DAG(
    dag_id="btc_from_api_to_posgres",
    schedule_interval='*/10 * * * *',
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["otus"],
) as dag:
    @task(task_id="connect_to_bitcoin_rate_api")
    def conn_bitcoin():
      try:
        URL_RESPONSE_BITСOIN.raise_for_status()
      except Exception as ex:
        print(ex)
      _log.info('connect to bitcoin_rate api')
    run1 = conn_bitcoin()   
    
    run3 = PostgresOperator(
             task_id = "put_to_postgresdb",
             postgres_conn_id = PG_CONN_ID,
             sql = put_to_psql(),
             dag=dag)
    

    run1 >> run3
