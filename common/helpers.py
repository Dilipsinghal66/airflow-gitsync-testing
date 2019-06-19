from datetime import datetime, timedelta

from airflow.contrib.hooks.mongo_hook import MongoHook
from airflow.contrib.hooks.redis_hook import RedisHook
from airflow.hooks.http_hook import HttpHook
from airflow.hooks.base_hook import BaseHook
from airflow.models import Connection
from models.twilio import ChatService
from redis import StrictRedis


from config import local_tz

redis_conn_callback: StrictRedis = RedisHook(redis_conn_id="redis_callback").get_conn()
redis_conn_twilio_message: StrictRedis = RedisHook(redis_conn_id="redis_twilio_message").get_conn()
twilio_cred_connections: Connection = BaseHook(source=None).get_connection(conn_id="twilio_credentials")


def create_health_plan():
    pass


def do_level_jump():
    pass


def get_user_by_filter(user_filter, projection=None, single=False):
    user_db = MongoHook(conn_id="mongo_user_db").get_conn().get_default_database()
    user_coll = user_db.get_collection("user")
    if single:
        user_data = user_coll.find_one(user_filter)
    else:
        user_data = user_coll.find(user_filter)
    return user_data


def update_user_activity(endpoint=None, payload=None):
    extra_options = {
        "check_response": True
    }
    activity_http_hook = HttpHook(method="PATCH", http_conn_id="http_services_url")
    activity_http_hook.run(endpoint=endpoint, data=payload, extra_options=extra_options)


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
    extra_args = twilio_cred_connections.extra_dejson
    chat_obj = ChatService(**extra_args)
    process_num_messages = 10
    twilio_message_redis_key = "twilio_message_list"
    while process_num_messages:
        if not redis_conn_twilio_message.exists(twilio_message_redis_key):
            process_num_messages = 0
            continue
        if not redis_conn_twilio_message.llen(twilio_message_redis_key):
            process_num_messages = 0
            continue
        send_message_data = redis_conn_twilio_message.lpop(twilio_message_redis_key)
        chat_obj.send_message(send_message_data)


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
        from common.statemachine import sendStateMachineMessage
        sendStateMachineMessage(callback_data)
    except Exception as e:
        redis_conn_callback.lpush(callback_key, callback_cached_data)
