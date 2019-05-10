import json
from datetime import timedelta, datetime
from time import sleep

from airflow.contrib.hooks.mongo_hook import MongoHook
from airflow.hooks.http_hook import HttpHook
from airflow.models import Variable

def get_patient_id_for_incomplete_task(task_lookup):
    health_plan_lookup = {
        "current_level": "Level " + str(task_lookup.get("level"))
    }
    goal_db = MongoHook(conn_id="mongo_goal_db").get_conn().get_default_database()
    health_plan = goal_db.get_collection("health-plan")
    health_plan_data = health_plan.find(health_plan_lookup, {"patientId": 1}).batch_size(100)
    level_patient_id_list = []
    while health_plan_data.alive:
        for health_plan in health_plan_data:
            patient_id = health_plan.get("patientId")
            if patient_id in level_patient_id_list:
                continue
            level_patient_id_list.append(patient_id)
    today = datetime.now().replace(hour=23, minute=59, second=59, microsecond=0) - timedelta(days=1)
    task_lookup["_created"] = {"$gt": today}
    task_lookup["patientId"] = {"$in": level_patient_id_list}
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
    exclude_user_list = list(map(int, Variable.get("exclude_user_ids", "0").split(",")))
    processing_batch_size = int(Variable.get("processing_batch_size", 100))
    payload = kwargs.pop("payload")
    task_lookup = kwargs.pop("task_details")
    user = user_db.get_collection("user")
    patient_id_list = get_patient_id_for_incomplete_task(task_lookup=task_lookup)
    user_filter = {
        "patientId": {"$nin": patient_id_list},
        "userStatus": {"$in": [11, 12, 13]}
    }
    user_data = user.find(user_filter, {"userId": 1}).batch_size(100)
    http_hook = HttpHook(
        method="POST",
        http_conn_id="chat_service_url"
    )
    user_id_list = []
    while user_data.alive:
        for user in user_data:
            user_id = user.get("userId")
            if test_user_id and int(user_id) != test_user_id:
                continue
            if int(user_id) in exclude_user_list:
                continue
            user_id_list.append(user_id)

    for i in range(0, len(user_id_list) + 1, processing_batch_size):
        _id_list = user_id_list[i:i + processing_batch_size]
        sleep(1)
        for user_id in _id_list:
            try:
                http_hook.run(endpoint="/api/v1/chat/user/" + str(round(user_id)) + "/message",
                              data=json.dumps(payload))
            except Exception as e:
                raise ValueError(str(e))
