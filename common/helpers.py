import json
from datetime import datetime
from time import sleep

from airflow.contrib.hooks.redis_hook import RedisHook
from airflow.models import Variable
from bson import ObjectId
from dateutil import parser

from common.db_functions import get_data_from_db
from common.http_functions import make_http_request
from common.twilio_helpers import get_twilio_service, \
    process_switch

active_cm_list = Variable().get(key="active_cm_list",
                                deserialize_json=True)


def send_chat_message(user_id=None, payload=None):
    try:
        endpoint = "user/" + str(
            round(user_id)) + "/message"
        print(endpoint)
        status, body = make_http_request(
            conn_id="http_chat_service_url",
            endpoint=endpoint, method="POST", payload=payload)
        print(status, body)
    except Exception as e:
        raise ValueError(str(e))


def mongo_query_builder(query_data):
    query = query_data.get("query")
    field = query.get("field")
    op = query.get("op")
    value = query.get("value")
    value_type = query.get("value_type")
    if value_type == "date":
        value = parser.parse(value)
    mongo_query = {
        field: {
            op: value
        }
    }
    return mongo_query


def process_dynamic_task(**kwargs):
    action = "dynamic_message"
    mongo_filter_field = None
    mongo_query = kwargs.get("query", {}).get("mongo", None)
    print(mongo_query)
    print(type(mongo_query))
    mongo_query = json.loads(mongo_query)
    print(mongo_query)
    sql_query = kwargs.get("query", {}).get("sql", None)
    message: str = kwargs.get("message")
    sql_data = None
    if sql_query:
        sql_data = get_data_from_db(db_type="mysql", conn_id="mysql_monolith",
                                    sql_query=sql_query)
    collection = mongo_query.get("collection")
    _filter = mongo_query_builder(query_data=mongo_query)
    print(_filter)
    mongo_data = None
    if mongo_query:
        mongo_data = get_data_from_db(conn_id="mongo_user_db",
                                      collection=collection, filter=_filter)
    patient_id_list = []
    message_replace_data = {}
    if sql_data:
        for patient in sql_data:
            patient_id = patient[0]
            patient_id_list.append(patient_id)
            message_replace_data[patient_id] = patient

    if mongo_data:
        for patient in mongo_data:
            if "patientId" not in patient.keys():
                mongo_filter_field = "_id"
            patient_id = patient.get(mongo_filter_field)
            patient_id_list.append(patient_id)
    print(patient_id_list)
    _filter = {
        mongo_filter_field: {"$in": patient_id_list}
    }
    projection = {
        "userId": 1, "patientId": 1, "_id": 0
    }
    user_data = get_data_from_db(conn_id="mongo_user_db", collection="user",
                                 filter=_filter, projection=projection)
    payload = {
        "action": action,
        "message": "",
        "is_notification": False
    }
    for user in user_data:
        user_id = user.get("userId")
        patient_id = user.get("patientId")
        patient_data = message_replace_data.get(patient_id)
        for i in range(0, len(patient_data)):
            old = "#" + str(i) + "#"
            new = str(patient_data[i])
            patient_message = message.replace(old, new)
        payload["message"] = patient_message
        # send_chat_message(user_id=user_id, payload=payload)
    print(sql_data)


def process_health_plan_not_created(patient_list):
    _filter = {
        "patientId": {"$in": patient_list}
    }
    projection = {
        "patientId": 1, "_id": 0
    }
    health_plan_data = get_data_from_db(conn_id="mongo_goal_db",
                                        collection="health_plan",
                                        filter=_filter, projection=projection)
    p_list = []
    for data in health_plan_data:
        p_list.append(data.get("patientId"))
    health_plan_missing = set(patient_list).difference(p_list)
    if health_plan_missing:
        for patient_id in health_plan_missing:
            print("Creating health plan for ", patient_id)
            payload = {
                "patientId": patient_id
            }
            make_http_request(conn_id="http_healthplan_url", method="POST",
                              payload=payload)
    else:
        print("Health plan created for all patients. Nothing to do. ")
    return patient_list


def find_patients_not_level_jumped(patient_list):
    print("Starting level jump of patients. ")
    _filter = {"current_level": {"$in": ["Level 1", "Level 2"]},
               "patientId": {"$in": patient_list}}
    projection = {
        "patientId": 1, "_id": 0
    }
    health_plan_data = get_data_from_db(conn_id="mongo_goal_db",
                                        collection="health_plan",
                                        filter=_filter, projection=projection)
    patient_list = []
    for data in health_plan_data:
        patient_id = data.get("patientId")
        if patient_id:
            print("Adding patient id  " + str(patient_id) + " for level jump")
            patient_list.append(patient_id)
    if not patient_list:
        print("No level jump required. All done. ")
    return patient_list


def get_patients_activated_today():
    today = datetime.utcnow().replace(hour=0, minute=0, second=0,
                                      microsecond=0)
    _filter = {"userStatus": 4,
               "userFlags.active.activatedOn": {"$gt": today}}
    projection = {"patientId": 1, "_id": 0}
    sort = [["userFlags.active.activatedOn", -1]]
    user_data = get_data_from_db(conn_id="mongo_user_db", collection="user",
                                 filter=_filter, projection=projection,
                                 sort=sort)
    patient_list = []
    for user in user_data:
        patient_list.append(user.get("patientId"))
    return patient_list


def get_deactivated_patients():
    """
    Function to return all users that have been marked as deactivated but not
    deleted from mongodb database.

    :return: None
    """
    _filter = {
        "userStatus": 3,
        "deleted": False
    }
    user_data = get_data_from_db(conn_id="mongo_user_db", collection="user",
                                 filter=_filter)
    return user_data


def level_jump_patient():
    patient_list = get_patients_activated_today()
    print("Activated patients ", patient_list)
    process_health_plan_not_created(patient_list=patient_list)
    patient_list = find_patients_not_level_jumped(patient_list=patient_list)
    payload = {
        "level": "Level 3"
    }
    if not patient_list:
        print("No patients received for level jump")
    for patient in patient_list:
        endpoint = str(patient) + "/level"
        print("Level jump for ", endpoint)
        status, data = make_http_request(conn_id="http_healthplan_url",
                                         method="PATCH",
                                         payload=payload, endpoint=endpoint)
        print(status, data)


def switch_active_cm():
    service = get_twilio_service()
    _filter = {"userStatus": 4, "assignedCm": {"$nin": active_cm_list}}
    switchable_users = get_data_from_db(conn_id="mongo_user_db",
                                        filter=_filter, collection="user")
    for user in switchable_users:
        active_cm = process_switch(user=user, service=service)
        if not active_cm:
            continue
        user_endpoint = str(user.get("_id"))
        try:
            payload = {
                "assignedCm": active_cm
            }
            make_http_request(conn_id="http_user_url", method="PATCH",
                              endpoint=user_endpoint, payload=payload)
        except Exception as e:
            print(e)
            sleep(5)
            try:
                make_http_request(conn_id="http_user_url", method="PATCH",
                                  endpoint=user_endpoint, payload=payload)
            except Exception as e:
                print(e)
                sleep(5)
                try:
                    make_http_request(conn_id="http_user_url",
                                      method="PATCH",
                                      endpoint=user_endpoint,
                                      payload=payload)
                except Exception as e:
                    print(e)
                    print("Failed to update channel for " + user_endpoint)
    if switchable_users:
        try:
            refresh_active_user_redis()
        except Exception as e:
            print(e)


def twilio_cleanup_channel(twilio_service=None, channel_sid=None):
    """
    This function fetches all members of the target channel defined in
    `channel_sid` and deletes the same from the channel.

    :param twilio_service: twilio chat service instance
    :param channel_sid: twilio channel specific to user
    :return: None
    """
    print("Cleaning up twilio channel " + channel_sid + " of all members.")
    channel = twilio_service.channels.get(sid=channel_sid)
    members = channel.members.list()
    if members:
        for member in members:
            member.delete()
    print(channel_sid + " cleaned of all members.")


def twilio_delete_user(twilio_service=None, user_sid=None):
    """
    This function fetches the user in twilio given by `user_sid` and deletes it

    :param twilio_service: twilio chat service instance
    :param user_sid: twilio user sid of the user
    :return:
    """
    print("Deleting deactivated twilio user " + user_sid)
    user = twilio_service.users.get(user_sid)
    user.delete()
    print("Deleted deactivated twilio user" + user_sid)


def mark_user_deleted(_id):
    """
    This function makes an api call to user service to mark the user specified
    by `_id` as deleted.
    :param _id: ObjectId mongo _id of the user to be deleted
    :return: None
    """
    print("Marking user with id " + _id + " as deleted in user service")
    make_http_request(conn_id="http_user_url", method="DELETE", endpoint=_id)
    print("User with id " + _id + " marked as deleted in user service")


def twilio_cleanup():
    """
    Python callable used for twilio cleanup dag. This function fetches
    deactivated patients, deletes members from each patient in twilio
    and marks patient as deleted in user service.

    - fetch deactivated patients
    - remove members from patient's channel
    - remove patient's user from twilio
    - mark patient as deleted in user service

    Deactivated patients condition
    if user.userStatus == INACTIVE and user.deleted = False

    :return: None
    """
    print("Fetching users deactivated but not deleted. ")
    users_deactivated = get_deactivated_patients()
    if users_deactivated:
        print("Deactivated users fetched. Proceeding to deletion")
        twilio_service = get_twilio_service()
    else:
        print("No new deleted users found. Nothing to do")
        return
    for user in users_deactivated:
        patient_id = user.get("patient_id")
        _id = str(user.get("_id"))
        print("Processing deletion for deactivated patient " + str(
            patient_id) + " with id " + _id)
        chat_information = user.get("chatInformation", {})
        provider_data = chat_information.get("providerData", {})
        channel_sid = provider_data.get("channelSid", None)
        if not channel_sid:
            print("Error in user data for " + str(
                patient_id) + ". Missing channel information")
        user_sid = provider_data.get("userSid", None)
        if not user_sid:
            print("Error in user data for " + str(
                patient_id) + ". Missing twilio user information")
        try:
            twilio_cleanup_channel(twilio_service=twilio_service,
                                   channel_sid=channel_sid)
        except Exception as e:
            print(e)
        try:
            twilio_delete_user(twilio_service=twilio_service,
                               user_sid=user_sid)
        except Exception as e:
            print(e)
        try:
            mark_user_deleted(_id=_id)
        except Exception as e:
            print(e)
    print("Finished processing deactivated users for deletion. ")


def sanitize_data(data):
    if isinstance(data, ObjectId):
        return str(data)
    if isinstance(data, datetime):
        return str(data)
    if isinstance(data, dict):
        for k, v in data.items():
            data[k] = sanitize_data(v)
            if isinstance(v, list):
                v1 = []
                for d in v:
                    v1.append(sanitize_data(d))
                data[k] = v1
    return data


def refresh_active_user_redis():
    redis_hook = RedisHook(redis_conn_id="redis_active_users_chat")
    redis_conn = redis_hook.get_conn()
    for cm in active_cm_list:
        _filter = {"userStatus": 4, "assignedCm": cm}
        cacheable_users = get_data_from_db(conn_id="mongo_user_db",
                                           filter=_filter, collection="user")
        if cacheable_users:
            redis_conn.delete("active_users_"+str(cm))
        for user in cacheable_users:
            sanitized_data = json.dumps(sanitize_data(user))
            redis_conn.rpush("active_users_" + str(cm), sanitized_data)
