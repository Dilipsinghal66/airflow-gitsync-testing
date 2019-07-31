import json
from datetime import datetime
from random import choice
from time import sleep

from airflow.hooks.http_hook import HttpHook
from airflow.models import Variable
from twilio.rest import Client

from common.db_functions import get_data_from_db
from common.http_functions import make_http_request


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
    return patient_list


def find_patients_not_level_jumped(patient_list):
    _filter = {"current_level": {"$in": ["Level 1", "Level 2"]},
               "patientId": {"$in": patient_list}}
    projection = {
        "patientId": 1, "_id": 0
    }
    health_plan_data = get_data_from_db(conn_id="mongo_goal_db",
                                        collection="health-plan",
                                        filter=_filter, projection=projection)
    patient_list = []
    for data in health_plan_data:
        patient_list.append(data.get("patientId"))
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
    for patient in patient_list:
        endpoint = str(patient) + "/level"
        print("Level jump for ", endpoint)
        status, data = make_http_request(conn_id="http_healthplan_url",
                                         method="PATCH",
                                         payload=payload, endpoint=endpoint)
        print(status, data)


def switch_active_cm():
    active_cm_list = Variable().get(key="active_cm_list",
                                    deserialize_json=True)
    twilio_hook = HttpHook().get_connection(conn_id="http_twilio")
    account_sid = twilio_hook.login
    auth_token = twilio_hook.password
    service_sid = twilio_hook.extra_dejson.get("service_sid")
    twilio_conn = Client(account_sid, auth_token)
    service = twilio_conn.chat.services.get(sid=service_sid)
    _filter = {"userStatus": 4, "assignedCm": {"$nin": active_cm_list}}
    active_cm_attributes = {
        "isCm": True,
        "activeCm": True
    }
    switchable_users = get_data_from_db(conn_id="mongo_user_db",
                                        filter=_filter, collection="user")
    for user in switchable_users:
        active_cm_exists = False
        user_channel = user.get("chatInformation", {}).get("providerData",
                                                           {}).get(
            "channelSid", None)
        user_identity = user.get("chatInformation", {}).get("providerData",
                                                            {}).get("identity",
                                                                    None)
        print(user_identity)
        user_endpoint = str(user.get("_id"))
        if not user_channel or not user_identity:
            continue
        channel = service.channels(user_channel).fetch()
        members = channel.members.list()
        for member in members:
            if not int(member.identity) == int(user_identity):
                print(member.attributes)
                attributes = json.loads(member.attributes)
                active_cm = attributes.get("activeCm")
                if active_cm:
                    if member.identity in active_cm_list:
                        active_cm_exists = True
                        continue
                print("Deleting member " + member.identity +
                      "from channel " + user_channel)
                member.delete()
                print("Member deleted" + member.identity +
                      "from channel" + user_channel)
        if not active_cm_exists:
            active_cm = choice(active_cm_list)
            print("Active member adding to " + user_channel +
                  "with identity " + active_cm)
            channel.members.create(active_cm,
                                   attributes=json.dumps(
                                       active_cm_attributes))
            try:
                payload = {
                    "assignedCm": active_cm
                }
                make_http_request(conn_id="http_user_url", method="PATCH",
                                  endpoint=user_endpoint, payload=payload)
            except Exception as e:
                sleep(5)
                try:
                    make_http_request(conn_id="http_user_url", method="PATCH",
                                      endpoint=user_endpoint, payload=payload)
                except Exception as e:
                    sleep(5)
                    try:
                        make_http_request(conn_id="http_user_url",
                                          method="PATCH",
                                          endpoint=user_endpoint,
                                          payload=payload)
                    except Exception as e:
                        print("Failed to update channel for " + user_channel)
            print(
                "Active member added to " + user_channel +
                "with identity " + active_cm)
