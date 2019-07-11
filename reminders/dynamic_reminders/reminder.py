import json
from datetime import date

import requests
from airflow.contrib.hooks.mongo_hook import MongoHook
from airflow.hooks.http_hook import HttpHook
from airflow.models import Variable
from dateutil import parser


def get_patient_days(patient):
    days = None
    activated_on = patient.get("userFlags", {}).get("active", {}).get(
        "activatedOn", None)
    if not activated_on:
        return days
    if isinstance(activated_on, str):
        activated_on = parser.parse(activated_on)
    activated_date = activated_on.date()
    today = date.today()
    date_diff = today - activated_date
    days = date_diff.days
    days = days + 1
    return days


def get_parsed_resource_data(resource_url: str):
    data = None
    try:
        response = requests.get(resource_url)
        data = response.text
        data = json.loads(data)
    except Exception as e:
        print(str(e))
    return data


def refresh_daily_message():
    dynamic_message_url = "https://services.zyla.in/statemachine/scheduler/dynamic/message/today"
    dynamic_message_list = get_parsed_resource_data(dynamic_message_url)
    return dynamic_message_list


def send_reminder(**kwargs):
    time_data_url = "https://services.zyla.in/statemachine/scheduler/21:45/messages/4"
    time_data = get_parsed_resource_data(time_data_url)
    messages = time_data.get("messages")
    dynamic_messages = refresh_daily_message()
    message = None
    action = time_data.get("action")
    user_db = MongoHook(
        conn_id="mongo_user_db").get_conn().get_default_database()
    test_user_id = int(Variable.get("test_user_id", '0'))
    exclude_user_list = list(
        map(int, Variable.get("exclude_user_ids", "0").split(",")))
    payload = {
        "action": action,
        "message": message,
        "is_notification": False
    }
    user = user_db.get_collection("user")
    user_filter = {
        "userStatus": {"$in": [4]}
    }
    user_data = user.find(user_filter).batch_size(100)
    http_hook = HttpHook(
        method="POST",
        http_conn_id="chat_service_url"
    )
    while user_data.alive:
        for user in user_data:
            patient_days = get_patient_days(patient=user)
            if not patient_days:
                continue
            patient_days = patient_days - 1
            if patient_days <= 6:
                message = messages[patient_days]
            if patient_days > 6:
                if not len(dynamic_messages):
                    continue
                message = dynamic_messages[0]
            payload["message"] = message
            user_id = user.get("userId")
            if test_user_id and int(user_id) != test_user_id:
                continue
            if int(user_id) in exclude_user_list:
                continue
            try:
                print(payload)
                http_hook.run(endpoint="/api/v1/chat/user/" + str(
                    round(user_id)) + "/message",
                              data=json.dumps(payload))
            except Exception as e:
                raise ValueError(str(e))
