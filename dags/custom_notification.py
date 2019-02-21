from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.hooks.mongo_hook import MongoHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import (
    BranchPythonOperator,
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

def python_task(*args, **kwargs):
    print(args)
    print(kwargs)
    return "finished"

task_even = None
task_odd = None

def run_task(**kwargs):
    for d in kwargs:
        user_id: int = d.get("userId", None)
        if user_id:
            if user_id % 2 == 0:
                print("even")
            else:
                print("odd")
    return "finished"


dag = DAG("custom-notifications", default_args=default_args,
          schedule_interval='*/30 * * * * *', max_active_runs=5)
run_this_first = DummyOperator(
    task_id='run_this_first',
    dag=dag,pool="test"
)

cond_op = BranchPythonOperator(
 task_id="task_condition",
 provide_context=True,
 python_callable=run_task,
 dag=dag,
 pool="test"
)

run_this_first >> cond_op

join = DummyOperator(
    task_id='join',
    trigger_rule='one_success',
    dag=dag,
    pool="test"
)

for user in users:
    user_id = user.get("userId")
    t = PythonOperator(task_id=str(user_id)+"_follow", dag=dag,
                       python_callable=run_task, op_kwargs=user, pool="test")

    cond_op >> t >> join
