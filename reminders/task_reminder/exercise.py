import json
from copy import deepcopy
from datetime import datetime, timedelta
from time import sleep

import urllib3
from airflow import DAG
from airflow.contrib.hooks.mongo_hook import MongoHook
from airflow.hooks.http_hook import HttpHook
from airflow.operators.python_operator import PythonOperator
from airflow.utils import dates
from urllib3.exceptions import InsecureRequestWarning

urllib3.disable_warnings(InsecureRequestWarning)

default_args = {
    'owner': 'airflow',
    'start_date': dates.days_ago(2),
    'depends_on_past': False,
    'email': 'mrigesh@zyla.in',
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)

}

dag = DAG(
    dag_id='exercise_reminder',
    default_args=default_args,
    schedule_interval="30 19 * * *",
    catchup=False
)

payload = {
    "action": "tasks_reporting_14_00",
    "is_notification": False,
    "message": "I am proud of your determination! Please also share the tasks you completed today"
}

headers = {
    "client": "service",
    "access_token": "lKKqOArIvHczgW5w4r9NMF1y41kpXs2v",
    "Content-Type": "application/json"
}


def send_reminder(**kwargs):
    dag.log.info("Starting task")
    user_db = MongoHook(
        conn_id="mongo_user_db"
    ).get_conn().get_default_database()
    goal_db = MongoHook(
        conn_id="mongo_goal_db"
    ).get_conn().get_default_database()
    message = "Report the number of total minutes spent on walking or exercising today."
    payload["message"] = message
    today = datetime.now().replace(hour=23, minute=59, second=59, microsecond=0) - timedelta(days=1)
    task_filter_payload = deepcopy(kwargs)
    task_filter_payload["_created"] = {"$gt": today}
    user = user_db.get_collection("user")
    tasks = goal_db.get_collection("goal")
    tasks_data = tasks.find(task_filter_payload, {"patientId": 1})
    patient_id_list = []
    for tasks in tasks_data:
        patient_id = tasks.get("patientId")
        patient_id_list.append(patient_id)
    user_filter = {
        "patientId": {"$nin": patient_id_list},
        "userStatus": {"$in": [11, 12, 13]}
    }
    user_data = user.find(user_filter, {"userId": 1}).batch_size(100)
    http_hook = HttpHook(
        method="POST",
        http_conn_id="chat_service_url"
    )
    while user_data.alive:
        for user in user_data:
            sleep(1)
            user_id = user.get("userId")
            try:
                http_hook.run(endpoint="/api/v1/chat/user/" + str(user_id) + "/message", data=json.dumps(payload),
                              headers=headers)
            except Exception as e:
                dag.log.error(e)
                raise ValueError("Task failed")
    return True


reminder_7_30 = PythonOperator(
    task_id="reminder_07_30",
    task_concurrency=1,
    python_callable=send_reminder,
    dag=dag,
    op_kwargs={"taskId": 4180},
    pool="task_reminder_pool"
)
