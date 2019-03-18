import json
from datetime import timedelta, datetime
from time import sleep

from airflow.contrib.hooks.mongo_hook import MongoHook
from airflow.hooks.http_hook import HttpHook
from airflow.models import Variable


def get_patient_id_for_incomplete_task(task_lookup):
    goal_db = MongoHook(conn_id="mongo_goal_db").get_conn().get_default_database()
    today = datetime.now().replace(hour=23, minute=59, second=59, microsecond=0) - timedelta(days=1)
    task_lookup["_created"] = {"$gt": today}
    tasks = goal_db.get_collection("tasks_report")
    tasks_data = tasks.find(task_lookup, {"patientId": 1}).batch_size(100)
    patient_id_list = []
    while tasks_data.alive:
        for tasks in tasks_data:
            patient_id = tasks.get("patientId")
            if patient_id in patient_id_list:
                continue
            patient_id_list.append(patient_id)
    return patient_id_list


def send_reminder(**kwargs):
    user_db = MongoHook(conn_id="mongo_user_db").get_conn().get_default_database()
    test_user_id = int(Variable.get("test_user_id", '0'))
    payload_var = kwargs.pop("payload_var")
    payload = Variable.get(payload_var, deserialize_json=True)
    user = user_db.get_collection("user")
    patient_id_list = get_patient_id_for_incomplete_task(kwargs)
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
            sleep(0.5)
            user_id = user.get("userId")
            if test_user_id and int(user_id) != test_user_id:
                continue
            try:
                http_hook.run(endpoint="/api/v1/chat/user/" + str(user_id) + "/message", data=json.dumps(payload))
            except Exception as e:
                raise ValueError(str(e))
