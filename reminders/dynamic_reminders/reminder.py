import calendar
import random
from datetime import timedelta, datetime
from time import sleep

from airflow.models import Variable
from airflow.utils.log.logging_mixin import LoggingMixin

from common.db_functions import get_data_from_db
from common.helpers import send_chat_message, get_patient_on_trial_days, \
    get_patient_days, get_meditation_for_today, refresh_daily_message
from common.http_functions import make_http_request

calendar.setfirstweekday(6)
log = LoggingMixin().log

count_threshold = int(Variable().get(key="request_count_threshold",
                                     deserialize_json=True))
time_delay = int(
    Variable().get(key="request_time_delay", deserialize_json=True))


def send_notifications(time=None, reminder_type=None, index_by_days=False):
    if not time or not reminder_type:
        return
    time_data_endpoint = time + "/messages/" + str(reminder_type)
    status, time_data = make_http_request(conn_id="http_statemachine_url",
                                          endpoint=time_data_endpoint,
                                          method="GET")
    messages = time_data.get("messages")
    message = random.choice(messages)
    action = time_data.get("action")
    test_user_id = int(Variable.get("test_user_id", '0'))
    exclude_user_list = list(
        map(int, Variable.get("exclude_user_ids", "0").split(",")))
    payload = {
        "action": action,
        "message": message,
        "is_notification": False
    }
    notification_filter_by_days = int(
        Variable.get("notification_filter_by_days", '15'))
    filter_date = datetime.utcnow() - timedelta(
        days=notification_filter_by_days)
    user_filter = {
        "userStatus": {"$in": [11, 12]},
        "_created": {"$gt": filter_date},
        "countryCode": {"$in": [91]},
        "docCode": {"$regex": "^ZH"}
    }
    if test_user_id:
        user_filter["userId"] = test_user_id
    user_data = get_data_from_db(conn_id="mongo_user_db", collection="user",
                                 filter=user_filter, batch_size=100)
    request_count = 0
    while user_data.alive:
        for user in user_data:
            if index_by_days:
                message_index = get_patient_on_trial_days(patient=user)
                patient_id = str(user.get("patientId"))
                if not message_index:
                    log.warning(
                        "Failed to get patient days for patient " + patient_id)
                    continue
                message_index -= 1
                if -1 < message_index < len(messages):
                    try:
                        message = messages[message_index]
                        if not message or message == "null":
                            continue
                    except Exception as e:
                        print(e)
                        continue
                else:
                    continue
            user_id = user.get("userId")
            if test_user_id and int(user_id) != test_user_id:
                continue
            if int(user_id) in exclude_user_list:
                continue
            payload["message"] = message
            try:
                send_chat_message(user_id=user_id, payload=payload)
            except Exception as e:
                print(e)
            if request_count >= count_threshold:
                print("Request limit reached. Stopping for " + str(
                    time_delay) + " milliseconds")
                sleep(time_delay / 1000)
                request_count = 0
            else:
                request_count += 1


def send_dynamic(time=None, reminder_type=None, index_by_days=False):
    if not time or not reminder_type:
        return
    time_data_endpoint = time + "/messages/" + str(reminder_type)
    status, time_data = make_http_request(conn_id="http_statemachine_url",
                                          endpoint=time_data_endpoint,
                                          method="GET")
    messages = time_data.get("messages")
    dynamic_messages = refresh_daily_message()
    message = None
    action = time_data.get("action")
    test_user_id = int(Variable.get("test_user_id", '0'))
    exclude_user_list = list(
        map(int, Variable.get("exclude_user_ids", "0").split(",")))
    payload = {
        "action": action,
        "message": message,
        "is_notification": False
    }
    user_filter = {
        "userStatus": {"$in": [4]},
        "countryCode": {"$in": [91]},
        "docCode": {"$regex": "^ZH"}
    }
    if test_user_id:
        user_filter["userId"] = test_user_id
    user_data = get_data_from_db(conn_id="mongo_user_db", collection="user",
                                 filter=user_filter, batch_size=100)
    request_count = 0
    while user_data.alive:
        for user in user_data:
            user_id = user.get("userId")
            if test_user_id and int(user_id) != test_user_id:
                continue
            if int(user_id) in exclude_user_list:
                continue
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
            send_chat_message(user_id=user_id, payload=payload)
            if request_count >= count_threshold:
                print("Request limit reached. Stopping for " + str(
                    time_delay) + " milliseconds")
                sleep(time_delay / 1000)
                request_count = 0
            else:
                request_count += 1


def send_meditation(**kwargs):
    meditation_schedule = kwargs.get("schedule")
    test_user_id = int(Variable.get("test_user_id", '0'))
    exclude_user_list = list(
        map(int, Variable.get("exclude_user_ids", "0").split(",")))
    meditation_id = get_meditation_for_today(
        meditation_schedule=meditation_schedule)
    if not meditation_id:
        return
    payload = {
        "action": "meditation",
        "message": str(meditation_id)
    }
    print(payload)
    user_filter = {
        "userStatus": {"$in": [4]},
        "countryCode": {"$in": [91]},
        "docCode": {"$regex": "^ZH"}
    }
    if test_user_id:
        user_filter["userId"] = test_user_id
    user_data = get_data_from_db(conn_id="mongo_user_db", collection="user",
                                 filter=user_filter, batch_size=100)
    request_count = 0
    while user_data.alive:
        for user in user_data:
            user_id = user.get("userId")
            if test_user_id and int(user_id) != test_user_id:
                continue
            if int(user_id) in exclude_user_list:
                continue
            send_chat_message(user_id=user_id, payload=payload)
            if request_count >= count_threshold:
                print("Request limit reached. Stopping for " + str(
                    time_delay) + " milliseconds")
                sleep(time_delay / 1000)
                request_count = 0
            else:
                request_count += 1
