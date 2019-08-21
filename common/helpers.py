import json
from datetime import datetime
from time import sleep

from airflow.models import Variable
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
            patient_id = patient.get("patientId")
            patient_id_list.append(patient_id)
    print(patient_id_list)
    _filter = {
        "patientId": {"$in": patient_id_list}
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
