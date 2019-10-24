import calendar
import json
from datetime import datetime, date
from http import HTTPStatus
from random import choice
from time import sleep

from airflow.contrib.hooks.redis_hook import RedisHook
from airflow.models import Variable
from airflow.utils.log.logging_mixin import LoggingMixin
from bson import ObjectId
from dateutil import parser
from twilio.base.exceptions import TwilioRestException

from common.db_functions import get_data_from_db
from common.http_functions import make_http_request
from common.twilio_helpers import get_twilio_service, \
    process_switch, check_and_add_cm
from config import local_tz

enable_message = bool(int(Variable.get("enable_message", "1")))

log = LoggingMixin().log


def send_chat_message(user_id=None, payload=None):
    try:
        endpoint = "user/" + str(
            round(user_id)) + "/message"
        log.info(endpoint)
        if enable_message:
            status, body = make_http_request(
                conn_id="http_chat_service_url",
                endpoint=endpoint, method="POST", payload=payload)
            log.info(status, body)
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


def task_failure_callback(context):
    failure_payload = {
        "executed": True,
        "status": "failure"
    }
    task_instance = context.get("task")
    task_args = task_instance.op_kwargs
    task_mongo_id = task_args.get("_id", None)
    if task_mongo_id:
        task_mongo_id = str(task_mongo_id)
    endpoint = task_mongo_id
    make_http_request(conn_id="http_jobs_url", method="PATCH",
                      payload=failure_payload, endpoint=endpoint)


def task_success_callback(context):
    success_payload = {
        "executed": True,
        "status": "success"
    }
    task_instance = context.get("task")
    task_args = task_instance.op_kwargs
    task_mongo_id = task_args.get("_id", None)
    if task_mongo_id:
        task_mongo_id = str(task_mongo_id)
    endpoint = task_mongo_id
    make_http_request(conn_id="http_jobs_url", method="PATCH",
                      payload=success_payload, endpoint=endpoint)


def process_dynamic_task(**kwargs):
    action = "dynamic_message"
    mongo_filter_field = "patientId"
    mongo_query = kwargs.get("query", {}).get("mongo", None)
    sql_query = kwargs.get("query", {}).get("sql", None)
    message: str = kwargs.get("message")
    sql_data = None
    if sql_query:
        sql_data = get_data_from_db(db_type="mysql", conn_id="mysql_monolith",
                                    sql_query=sql_query, execute_query=True)
    mongo_data = None
    if mongo_query:
        mongo_query = json.loads(mongo_query)
        collection = mongo_query.get("collection")
        _filter = mongo_query_builder(query_data=mongo_query)
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
    log.info(patient_id_list)
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
        send_chat_message(user_id=user_id, payload=payload)


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
            log.info("Creating health plan for ", patient_id)
            payload = {
                "patientId": patient_id
            }
            make_http_request(conn_id="http_healthplan_url", method="POST",
                              payload=payload)
    else:
        log.info("Health plan created for all patients. Nothing to do. ")
    return patient_list


def find_patients_not_level_jumped(patient_list):
    log.info("Starting level jump of patients. ")
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
            log.info(
                "Adding patient id  " + str(patient_id) + " for level jump")
            patient_list.append(patient_id)
    if not patient_list:
        log.info("No level jump required. All done. ")
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
    log.info("Activated patients ", patient_list)
    process_health_plan_not_created(patient_list=patient_list)
    patient_list = find_patients_not_level_jumped(patient_list=patient_list)
    payload = {
        "level": "Level 3"
    }
    if not patient_list:
        log.info("No patients received for level jump")
    for patient in patient_list:
        endpoint = str(patient) + "/level"
        log.info("Level jump for ", endpoint)
        status, data = make_http_request(conn_id="http_healthplan_url",
                                         method="PATCH",
                                         payload=payload, endpoint=endpoint)
        log.info(status, data)


def get_cm_list_by_type(cm_type="active"):
    _filter = {
        "cmType": cm_type
    }
    projection = {
        "chatInformation.providerData.identity": 1,
        "cmId": 1,
        "_id": 0
    }

    sales_cm = get_data_from_db(conn_id="mongo_user_db", filter=_filter,
                                projection=projection,
                                collection="careManager")
    cm_list = [i for i in sales_cm]
    return cm_list


def add_sales_cm(cm_type):
    cm_list = get_cm_list_by_type(cm_type=cm_type)
    sales_cm = choice(cm_list)
    service = get_twilio_service()
    today = datetime.utcnow().replace(hour=0, minute=0, second=0,
                                      microsecond=0, tzinfo=local_tz)
    _filter = {
        "assignedCmType": "normal",
        "processedSales": {"$ne": True},
        "userStatus": {"$ne": 4},
        "_created": {"$gt": today}
    }
    eligible_users = get_data_from_db(conn_id="mongo_user_db",
                                      filter=_filter, collection="user")
    update_redis = False
    for user in eligible_users:
        check_and_add_cm(user=user, service=service, cm=sales_cm)
        endpoint = str(user.get("_id"))
        cm_id = sales_cm.get("cmId")
        payload = {
            "assignedCmType": "sales",
            "assignedCm": cm_id
        }
        status, body = make_http_request(conn_id="http_user_url",
                                         payload=payload, endpoint=endpoint,
                                         method="PATCH")
        if status != HTTPStatus.OK:
            print("failed to update sales cm for user ")
        update_redis = True
    if update_redis:
        try:
            refresh_cm_type_user_redis(cm_type=cm_type)
        except Exception as e:
            log.info(e)


def switch_active_cm(cm_type):
    cm_list = get_cm_list_by_type(cm_type=cm_type)
    active_cm_list = [i.get("cmId") for i in cm_list]
    service = get_twilio_service()
    _filter = {"userStatus": 4, "assignedCm": {"$nin": active_cm_list}}
    switchable_users = get_data_from_db(conn_id="mongo_user_db",
                                        filter=_filter, collection="user")
    update_redis = False
    for user in switchable_users:
        active_cm = process_switch(user=user, service=service)
        if not active_cm:
            continue
        user_endpoint = str(user.get("_id"))
        try:
            payload = {
                "assignedCm": active_cm,
                "assignedCmType": "active",
                "processedSales": True
            }
            make_http_request(conn_id="http_user_url", method="PATCH",
                              endpoint=user_endpoint, payload=payload)
            update_redis = True
        except Exception as e:
            log.info(e)
            sleep(5)
            try:
                make_http_request(conn_id="http_user_url", method="PATCH",
                                  endpoint=user_endpoint, payload=payload)
                update_redis = True
            except Exception as e:
                log.info(e)
                sleep(5)
                try:
                    make_http_request(conn_id="http_user_url",
                                      method="PATCH",
                                      endpoint=user_endpoint,
                                      payload=payload)
                    update_redis = True
                except Exception as e:
                    log.info(e)
                    log.info("Failed to update channel for " + user_endpoint)
    if update_redis:
        try:
            refresh_cm_type_user_redis(cm_type=cm_type)
        except Exception as e:
            log.info(e)


def twilio_cleanup_channel(twilio_service=None, channel_sid=None):
    """
    This function fetches all members of the target channel defined in
    `channel_sid` and deletes the same from the channel.

    :param twilio_service: twilio chat service instance
    :param channel_sid: twilio channel specific to user
    :return: None
    """
    log.info("Cleaning up twilio channel " + channel_sid + " of all members.")
    channel = twilio_service.channels.get(sid=channel_sid)
    members = channel.members.list()
    if members:
        for member in members:
            member.delete()
        log.info(channel_sid + " cleaned of all members.")
    else:
        log.info(channel_sid + " has no members to delete.")


def twilio_delete_user(twilio_service=None, user_sid=None):
    """
    This function fetches the user in twilio given by `user_sid` and deletes it

    :param twilio_service: twilio chat service instance
    :param user_sid: twilio user sid of the user
    :return:
    """
    log.info("Deleting deactivated twilio user " + user_sid)
    user = twilio_service.users.get(user_sid)
    user.delete()
    log.info("Deleted deactivated twilio user" + user_sid)


def mark_user_deleted(_id):
    """
    This function makes an api call to user service to mark the user specified
    by `_id` as deleted.
    :param _id: ObjectId mongo _id of the user to be deleted
    :return: None
    """
    log.info("Marking user with id " + _id + " as deleted in user service")
    try:
        make_http_request(conn_id="http_user_url", method="DELETE",
                          endpoint=_id)
        log.info("User with id " + _id + " marked as deleted in user service")
    except Exception as e:
        log.error(e, exc_info=True)


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
    log.info("Fetching users deactivated but not deleted. ")
    users_deactivated = get_deactivated_patients()
    if users_deactivated:
        log.info("Deactivated users fetched. Proceeding to deletion")
        twilio_service = get_twilio_service()
    else:
        log.info("No new deleted users found. Nothing to do")
        return
    for user in users_deactivated:
        patient_id = user.get("patientId")
        _id = str(user.get("_id"))
        log.info("Processing deletion for deactivated patient " + str(
            patient_id) + " with id " + _id)
        chat_information = user.get("chatInformation", {})
        provider_data = chat_information.get("providerData", {})
        channel_sid = provider_data.get("channelSid", None)
        if not channel_sid:
            log.info("Error in user data for " + str(
                patient_id) + ". Missing channel information")
        user_sid = provider_data.get("userSid", None)
        if not user_sid:
            log.info("Error in user data for " + str(
                patient_id) + ". Missing twilio user information")
        try:
            twilio_cleanup_channel(twilio_service=twilio_service,
                                   channel_sid=channel_sid)
        except Exception as e:
            log.error(e)
        try:
            twilio_delete_user(twilio_service=twilio_service,
                               user_sid=user_sid)
        except TwilioRestException as e:
            log.error(e.msg)
        except Exception as e:
            log.error(e)
        try:
            mark_user_deleted(_id=_id)
        except Exception as e:
            log.error(e)
    log.info("Finished processing deactivated users for deletion. ")


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


def refresh_cm_type_user_redis(cm_type="active"):
    cm_list = get_cm_list_by_type(cm_type=cm_type)
    cm_list = [i.get("cmId") for i in cm_list]
    redis_hook = RedisHook(redis_conn_id="redis_sales_users_chat")
    redis_conn = redis_hook.get_conn()
    for cm in cm_list:
        redis_key = cm_type + "_users_" + str(cm)
        _filter = {"assignedCmType": cm_type}
        cacheable_users = get_data_from_db(conn_id="mongo_user_db",
                                           filter=_filter, collection="user")
        if cacheable_users:
            redis_conn.delete(redis_key)
        for user in cacheable_users:
            sanitized_data = json.dumps(sanitize_data(user))
            redis_conn.rpush(redis_key, sanitized_data)


def get_care_managers():
    _filter = {
        "cmType": "normal"
    }
    projection = {
        "cmId": 1,
        "_id": 0,
    }
    cm_data = get_data_from_db(conn_id="mongo_cm_db",
                               collection="careManager",
                               projection=projection, filter=_filter)
    return cm_data


def create_cm(cm, tries=3):
    log.debug("Creating new cm on the basis of " + json.dumps(cm))
    cm_id = cm.get("cmId")
    cm_id_new = cm_id - 1
    log.debug("New care manager id " + str(cm_id_new))
    cm_payload = {}
    try:
        make_http_request(conn_id="http_create_cm_url", method="POST",
                          payload=cm_payload)
    except Exception as e:
        log.error(e)
        if tries:
            retry = tries - 1
            create_cm(cm, tries=retry)
        raise ValueError("Care Manager create failed. ")


def enough_open_slots(cm_list):
    slot_threshold = Variable().get(key="cm_available_avg_slot_threshold",
                                    deserialize_json=True)
    log.debug("Average slots that must be available for all CMs " + str(
        slot_threshold))
    log.debug(type(slot_threshold))
    cm_count = len(cm_list)
    log.info("Total available care managers " + str(cm_count))
    available_slots = 0
    for cm in cm_list:
        slots = cm.get("openSlots")
        available_slots += slots
    log.debug("Total available slots: " + str(available_slots))
    avg_available = round(available_slots / cm_count)
    if avg_available and avg_available > slot_threshold:
        log.debug("We have enough slots available above threshold")
        log.debug("Average available slots: " + str(avg_available))
        enough_slots = True
    else:
        log.warning("Available slots less than threshold")
        log.warning("Average available slots: " + str(avg_available))
        log.warning("Slot threshold required: " + str(slot_threshold))
        enough_slots = False
    return enough_slots


def compute_cm_priority(cm_list):
    per_cm_slot_threshold = Variable().get(key="per_cm_slot_threshold",
                                           deserialize_json=True)
    log.debug("Min slot threshold per cm " + str(per_cm_slot_threshold))
    log.debug(type(per_cm_slot_threshold))
    cm_list = list(
        filter(lambda d: d["openSlots"] > per_cm_slot_threshold, cm_list))
    log.debug("CM list after threshold computation ")
    log.debug(cm_list)
    cm_priority_list = sorted(cm_list, key=lambda i: i['openSlots'])
    log.debug("CM list after priority computation")
    log.debug(cm_list)
    return cm_priority_list


def add_care_manager():
    log.debug("Fetching care manager data from db. ")
    cm_data = get_care_managers()
    log.debug("Care managers fetched from db")
    log.debug(cm_data)
    log.debug("Init twilio service object ")
    twilio_service = get_twilio_service()
    log.debug("Twilio service object init successful ")
    cm_slot_list = []
    for cm in cm_data:
        identity = cm.get("chatInformation", {}).get("providerData", {}).get(
            "identity", None)
        log.debug("Computing open slots for cmid " + str(identity))
        cm_open_slots = 0
        if identity:
            twilio_user = twilio_service.users.get(identity)
            log.debug("Fetched twilio user for cm " + str(identity))
            try:
                twilio_user = twilio_user.fetch()
            except Exception as e:
                log.error(e)
                log.error(
                    "Twilio user for " + str(identity) + " failed to fetch")
                log.warning("Continuing as nothing to do")
                continue
            cm_joined_channels = twilio_user.joined_channels_count
            log.debug("Total channels joined by cm " + str(cm_joined_channels))
            cm_open_slots = 1000 - cm_joined_channels
            log.debug(
                "Open slots for " + str(identity) + " : " + str(cm_open_slots))

        cm_slot_list.append({
            "cmId": identity,
            "openSlots": cm_open_slots
        })
    log.debug("Care managers with slots opened for further processing")
    log.debug(cm_slot_list)
    log.debug("Computing cm list by priority")
    cm_by_priority = compute_cm_priority(cm_list=cm_slot_list)
    try:
        have_enough_slots = enough_open_slots(cm_list=cm_by_priority)
        if not have_enough_slots:
            raise ValueError("There are not enough slots. Create new CM")
        log.info("we have enough cm slots.Nothing to do further")
    except Exception as e:
        log.error(e)
        log.warning("We do not have enough open slots. Creating new CM")
        cm_top_priority = cm_by_priority[-1:][0]
        create_cm(cm=cm_top_priority)
    redis_hook = RedisHook(redis_conn_id="redis_cm_pool")
    redis_conn = redis_hook.get_conn()
    redis_conn.delete("cm:inactive_pool")
    for cm in cm_by_priority:
        cm_data = json.dumps(cm)
        redis_conn.rpush("cm:inactive_pool", cm_data)


def deactivate_patients(**kwargs):
    log.debug("Starting deactivation of patients")
    log.debug("Fetch deactivation list from variables. ")
    deactivation_list = Variable.get("deactivation_list",
                                     deserialize_json=True)
    log.debug("Deactivation list fetched from variables. ")
    if not len(deactivation_list):
        log.info("No patients to deactivate. Nothing to do.")
        return
    log.debug("Patients to be deactivated " + json.dumps(deactivation_list))
    _filter = {
        "patientId": {"$in": deactivation_list},
        "userStatus": {"$ne": 3}
    }
    projection = {
        "_id": 1
    }
    deactivation_id_list = get_data_from_db(conn_id="mongo_user_db",
                                            collection="user",
                                            filter=_filter,
                                            projection=projection)
    for patient in deactivation_id_list:
        _id = patient.get("_id")
        if isinstance(_id, ObjectId):
            _id = str(_id)
        endpoint = _id
        log.debug("Starting deactivation for " + endpoint)
        payload = kwargs
        method = "PATCH"
        try:
            log.debug(
                "Updating user status to deactivate for user " + endpoint)
            make_http_request(conn_id="http_user_deactivation_url",
                              method=method,
                              endpoint=endpoint,
                              payload=payload)
        except Exception as e:
            log.error(e)
            log.error("Deactivation failed for " + endpoint)


def get_dynamic_scheduled_message_time():
    endpoint = "time/list"
    status, data = make_http_request("http_statemachine_url",
                                     endpoint=endpoint, method="GET")
    if status == HTTPStatus.OK:
        schedulables = data.get("schedulable_times")
        return schedulables
    else:
        log.error(status)
        log.error(data)
        return None


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
        if month_calendar[i][day_of_week] and month_calendar[i][day_of_week] \
                < day_today:
            week_of_month += 1
    meditation = meditation_schedule[week_of_month][day_of_week]
    return meditation


def refresh_daily_message():
    dynamic_message_endpoint = "dynamic/message/today"
    status, dynamic_message_list = make_http_request(
        conn_id="http_statemachine_url", endpoint=dynamic_message_endpoint,
        method="GET")
    return dynamic_message_list
