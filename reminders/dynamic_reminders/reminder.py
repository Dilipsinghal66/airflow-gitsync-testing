import calendar
import random
from datetime import date, timedelta, datetime
from time import sleep

from airflow.models import Variable
from dateutil import parser

from common.db_functions import get_data_from_db
from common.helpers import send_chat_message
from common.http_functions import make_http_request

calendar.setfirstweekday(6)

count_threshold = int(Variable().get(key="request_count_threshold",
                                     deserialize_json=True))
time_delay = int(
    Variable().get(key="request_time_delay", deserialize_json=True))


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


def refresh_daily_message():
    dynamic_message_endpoint = "dynamic/message/today"
    status, dynamic_message_list = make_http_request(
        conn_id="http_statemachine_url", endpoint=dynamic_message_endpoint,
        method="GET")
    return dynamic_message_list


def get_patient_on_trial_days(patient):
    days = None
    status_transition = patient.get("statusTransition", [])
    if not status_transition:
        return days
    day_on_trial = [d for d in status_transition if
                    d.get("status", None) == 11]
    if not day_on_trial:
        return days
    day_on_trial = day_on_trial[0]
    transition_time = day_on_trial.get("transitionTime", None)
    if not transition_time:
        return days
    if isinstance(transition_time, str):
        transition_time = parser.parse(transition_time)
    transition_date = transition_time.date()
    today = date.today()
    date_diff = today - transition_date
    days = date_diff.days + 1
    return days


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
        "_created": {"$gt": filter_date}
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
        "userStatus": {"$in": [4]}
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


def get_meditation_for_today(meditation_schedule=None):
    today = date.today()
    month_calendar = calendar.monthcalendar(today.year, today.month)
    day_today = today.day
    week_of_month = 0
    day_of_week = 0
    for i in range(len(month_calendar)):
        if day_today in month_calendar[i]:
            day_of_week = month_calendar[i].index(day_today)
    for i in range(len(month_calendar)):
        if month_calendar[i][day_of_week] and month_calendar[i][
            day_of_week] < day_today:
            week_of_month += 1
    print(day_of_week, week_of_month)
    meditation = meditation_schedule[week_of_month][day_of_week]
    return meditation


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
        "userStatus": {"$in": [4]}
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
