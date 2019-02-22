from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.hooks.mongo_hook import MongoHook
from airflow.operators.python_operator import (
    PythonOperator
)


mongo = MongoHook()
database = "api_service_user"
collection = "user"
coll = mongo.get_collection(mongo_collection=collection, mongo_db=database)
users = coll.find().limit(100)

default_args = {
 'owner': 'airflow',
 'start_date': datetime(2015, 1, 19),
 'email': ['airflow@example.com'],
 'email_on_failure': False,
 'email_on_retry': False,
 'retries': 1,
 'retry_delay': timedelta(minutes=5),
 # 'queue': 'bash_queue',
 # 'pool': 'backfill',
 # 'priority_weight': 10,
 # 'end_date': datetime(2016, 1, 1),
}


def run_task(**kwargs):
    a = {"done": True}
    return a


dag = DAG("test-notification", default_args=default_args,
          schedule_interval='0 */5 * * * *', max_active_runs=5)

task_1 = PythonOperator(
 task_id="task_condition",
 provide_context=True,
 python_callable=run_task,
 dag=dag,
 pool="test"
)
