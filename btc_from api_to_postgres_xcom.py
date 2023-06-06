
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
from airflow.decorators import task
from airflow.operators.python import PythonOperator, ExternalPythonOperator, PythonVirtualenvOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

log = _log.getLogger(__name__)

PATH_TO_PYTHON_BINARY = sys.executable

BASE_DIR = tempfile.gettempdir()

PG_CONN_ID='postgres_otus'
PG_TABLE='bitcoin'

URL_RESPONSE_BITСOIN=requests.get('https://api.coincap.io/v2/rates/bitcoin')

def get_bitcoin_rate(**kwargs):
        res_dict = URL_RESPONSE_BITСOIN.json()
        bitcoin_rate = res_dict.get('data').get('rateUsd')
        timestamp = res_dict.get('timestamp')
        print(bitcoin_rate,timestamp)

        print(json.dumps(res_dict, indent=4))
        _log.info("get bitcoin rate")
        ti=kwargs['ti']
        ti.xcom_push("bitcoin_rate",bitcoin_rate)
        ti.xcom_push("timestamp",timestamp)
        return bitcoin_rate, timestamp

with DAG(
    dag_id="btc_from_api_to_posgres_xcom",
    # schedule_interval='*/10 * * * *',
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
    
  
    run2 = PythonOperator(task_id='get_bitcoin_rate',
                           python_callable=get_bitcoin_rate,
                           dag=dag)
    
    run3 = PostgresOperator(
             task_id = "put_to_postgresdb",
             postgres_conn_id = PG_CONN_ID,
             sql = """ CREATE TABLE IF NOT EXISTS {0} (BITCOIN_RATE INT, TIMESTAMP BIGINT);
                         INSERT INTO {0} (BITCOIN_RATE, TIMESTAMP) VALUES ({1}, {2});
                     """\
                    .format(PG_TABLE,XComArg(run2, key="bitcoin_rate"),XComArg(run2, key="timestamp")),
             dag=dag)

    # run4 = BashOperator(
    #          task_id = "bash_pull",
    #          bash_command='echo "bash pull" && '
    #          f'echo "The xcom pushed bitcoin_rate {XComArg(run2, key="bitcoin_rate")}" && '
    #          f'echo "The xcom pushed timestamp {XComArg(run2, key="timestamp")}" && '
    #          'echo "finished"',
    #          do_xcom_push=False)    

    run1 >> run2 >> run3# >> run4
