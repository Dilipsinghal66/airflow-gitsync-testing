import json
from datetime import datetime, timedelta

from airflow.contrib.hooks.mongo_hook import MongoHook
from airflow.contrib.hooks.redis_hook import RedisHook
from airflow.hooks.base_hook import BaseHook
from airflow.hooks.http_hook import HttpHook
from airflow.models import Connection
from airflow.models import Variable
from redis import StrictRedis

from config import local_tz, extra_http_options
from models.twilio import ChatService

redis_conn_callback: StrictRedis = RedisHook(redis_conn_id="redis_callback").get_conn()
redis_conn_twilio_message: StrictRedis = RedisHook(redis_conn_id="redis_twilio_message").get_conn()
twilio_cred_connections: Connection = BaseHook(source=None).get_connection(conn_id="twilio_credentials")
patient_status_mapping = Variable().get(key="patient_status_config", deserialize_json=True)


def create_health_plan():
    pass


def do_level_jump():
    pass


def send_chat_notification(userId, data, message):
    notify_http_hook = HttpHook(method="POST", http_conn_id="http_notification_endpoint")

    notify_endpoint = "/" + str(userId)
    payload = {
        "title": message,
        "description": json.dumps(data),
        "action": "CHAT"
    }
    notify_http_hook.run(endpoint=notify_endpoint, data=json.dumps(payload), extra_options=extra_http_options)


def get_user_by_filter(user_filter, projection=None, single=False):
    user_db = MongoHook(conn_id="mongo_user_db").get_conn().get_default_database()
    user_coll = user_db.get_collection("user")
    if single:
        user_data = user_coll.find_one(user_filter)
    else:
        user_data = user_coll.find(user_filter)
    return user_data


def update_user_activity(endpoint=None, payload=None):
    activity_http_hook = HttpHook(method="PATCH", http_conn_id="http_user_url")
    activity_http_hook.run(endpoint=endpoint, data=json.dumps(payload), extra_options=extra_http_options)


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


def delete_redis_key(redis_obj, key):
    redis_obj.delete(key)


def check_redis_key(redis_obj, key):
    redis_len = redis_obj.llen(key)
    if not redis_len:
        delete_redis_key(redis_obj, key)
    return True if redis_len else False


def check_redis_keys_exist(pattern=""):
    keys = redis_conn_twilio_message.keys(pattern=pattern)
    if not keys:
        return False
    return True


def send_twilio_message():
    extra_args = twilio_cred_connections.extra_dejson
    chat_obj = ChatService(**extra_args)
    keys = redis_conn_twilio_message.keys(pattern="*_send_twilio_message")
    if not keys:
        print("no message to be sent")
        return True
    for key in keys:
        key = key.decode()
        message_max_counter = 15
        while check_redis_key(redis_conn_twilio_message, key) and message_max_counter:
            print("processing data for key " + key)
            redis_data = redis_conn_twilio_message.lindex(key, 0)
            twilio_message = json.loads(redis_data.decode())
            channel_sid = twilio_message.get("channelSid")
            attributes = twilio_message.get("attributes")
            try:
                chat_obj.set_channel(channel_sid=channel_sid)
                chat_obj.send_message(attributes=attributes)
                redis_conn_twilio_message.lpop(key)
            except Exception as e:
                print(str(e))
                print("Message sending failed")
            message_max_counter -= 1


def send_pending_callback_messages():
    keys = redis_conn_callback.keys(pattern="*_callback")
    if not keys:
        print("no callbacks to be processed")
        return True
    for key in keys:
        key = key.decode()
        callback_max_counter = 20
        while check_redis_key(redis_conn_callback, key) and callback_max_counter:
            print("processing callback for key " + key)
            callback_cached_data = redis_conn_callback.lindex(key, 0)
            callback_data = callback_cached_data.decode()
            try:
                from common.statemachine import sendStateMachineMessage
                sendStateMachineMessage(callback_data)
                redis_conn_callback.lpop(key)
            except Exception as e:
                print(str(e))
                redis_conn_callback.lpush(key, callback_cached_data)
            callback_max_counter -= 1


def update_patient_status_on_sm(user_id, sm_action):
    if not user_id or not sm_action:
        return False
    patient_status_sm_map = patient_status_mapping.get("patient_status_sm_map")
    patient_status_codes = patient_status_mapping.get("patient_status_codes")
    patient_status = patient_status_sm_map.get(sm_action, None)
    if not patient_status:
        return False
    status_code = patient_status_codes.get(patient_status, None)
    if not status_code:
        return False
    payload = {
        "userStatus": status_code
    }
    user_endpoint = str(round(user_id))
    status_update_hook = HttpHook(method="PATCH", http_conn_id="http_user_url")
    status_update_hook.run(endpoint=user_endpoint, data=json.dumps(payload), extra_options=extra_http_options)
