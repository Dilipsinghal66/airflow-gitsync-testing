import json
from copy import deepcopy
from datetime import datetime, timedelta

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
    'start_date': dates.days_ago(2)
}

dag = DAG(
    dag_id='exercise_reminder',
    default_args=default_args,
    schedule_interval="@once",
)

payload = json.dumps({
    "action": "tasks_reporting_14_00",
    "is_notification": True,
    "message": "test"
})

headers = {
    "client": "service",
    "access_token": "lKKqOArIvHczgW5w4r9NMF1y41kpXs2v",
    "Content-Type": "application/json"
}


def send_reminder(**kwargs):
    hook = MongoHook(
        mongo_conn_id="mongo_user_db",
    )
    today = datetime.now().replace(hour=23, minute=59, second=59, microsecond=0) - timedelta(days=1)
    task_filter_payload = deepcopy(kwargs)
    task_filter_payload["_created"] = {"$gt": today}
    print(task_filter_payload)
    user_db = hook.get_collection("user", "userService")
    tasks = hook.get_collection("tasks", "goal_service")
    tasks_data = tasks.find(task_filter_payload, {"patientId": 1})
    patient_id_list = []
    for tasks in tasks_data:
        patient_id = tasks.get("patientId")
        patient_id_list.append(patient_id)
    print(patient_id_list)
    user_filter = {
        "patientId": {"$nin": patient_id_list},
        "userStatus": {"$ne": 3}
    }
    user_data = user_db.find(user_filter, {"userId": 1})
    user_id_list = []
    for user in user_data:
        print(user)
        user_id = user.get("userId")
        user_id_list.append(user_id)
    http_hook = HttpHook(
        method="POST",
        http_conn_id="zyla_feature"
    )
    for user_id in user_id_list:
        print("send message for user id ", user_id)
        http_hook.run(endpoint="/api/v1/chat/user/" + str(user_id) + "/message", data=payload,
                     headers=headers)
    pass

reminder_7_30 = PythonOperator(
    task_id="reminder_07_30",
    task_concurrency=1,
    python_callable=send_reminder,
    dag=dag,
    op_kwargs={"taskId": 4180}
)
