from datetime import datetime, timedelta

from airflow.contrib.hooks.mongo_hook import MongoHook
from airflow.contrib.hooks.redis_hook import RedisHook

from config import local_tz

redis_conn_callback = RedisHook(redis_conn_id="redis_callback")


def create_health_plan():
    pass


def do_level_jump():
    pass


def get_activated_patients(**kwargs):
    minutes_diff = kwargs.get("duration", 5)
    to_time = datetime.now(tz=local_tz).replace(second=0, microsecond=0)
    from_time = to_time - timedelta(minutes=minutes_diff)
    user_db = MongoHook(conn_id="mongo_user_db").get_conn().get_default_database()
    user = user_db.get_collection("user")
    user_filter = {
        "userFlags.active.activatedOn": {"$gt": to_time, "$lt": from_time},
        "userStatus": 4,
        "userFlags.active.activated": True
    }
    activated_patient_list = user.find(user_filter, projection={"patientId": 1, "_id": 0})
    patient_id_list = []
    for patient in activated_patient_list:
        _id = patient.get("patientId")
        if _id:
            patient_id_list.append(_id)
    return patient_id_list


def send_twilio_message():
    print("message sent")


def send_pending_callback_messages():
    keys = redis_conn_callback.keys(pattern="*_callback")
    callback_key = None
    if len(keys):
        callback_key = keys[0].decode()
    if not callback_key:
        return True
    callback_cached_data = redis_conn_callback.lindex(callback_key, 0)
    if not callback_cached_data:
        redis_conn_callback.delete(callback_key)
    callback_data = callback_cached_data.decode()
    try:
        print(callback_data)
        # sendStateMachineMessage(callback_data)
    except Exception as e:
        redis_conn_callback.lpush(callback_key, callback_cached_data)
