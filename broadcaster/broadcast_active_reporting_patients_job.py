from airflow.models import Variable
from airflow.utils.log.logging_mixin import LoggingMixin
from datetime import datetime, time, date, timedelta
from common.db_functions import get_data_from_db
from common.helpers import process_dynamic_message

log = LoggingMixin().log

"""
Broadcast active patients which shared report between specified time duration
"""


def broadcast_active_reporting_patients():

    process_broadcast_days_active_reporting = int(Variable.get
                                                  ("process_broadcast_"
                                                   "active_reporting_disable",
                                                   "0"))
    if process_broadcast_days_active_reporting == 1:
        return

    message = str(Variable.get("broadcast_active_reporting_patients_msg", ''))

    delta = int(Variable.get("broadcast_active_reporting_patients_delta_in_"
                             "days", '1'))
    start_time = int(Variable.get("broadcast_start_time in hours", '18'))
    end_time = int(Variable.get("broadcast_end_time in hours", '18'))
    start_time = time(start_time, 0)
    end_time = time(end_time, 0)
    start_date = date.today() - timedelta(days=delta)
    start_date_time = datetime.combine(start_date, start_time)
    end_date_time = datetime.combine(date.today(), end_time)
    _filter = {
        "lastReported": {"$gte": start_date_time, "$lte": end_date_time}
    }

    projection = {
        "_id": 1
    }
    try:
        userid_cursor = get_data_from_db(conn_id="mongo_user_db",
                                         collection="user_activity",
                                         filter=_filter,
                                         projection=projection)
    except Exception as e:
        warning_message = "Connection with mongodb unsuccessful"
        log.warning(warning_message)
        log.error(e, exc_info=True)
        raise e

    mongo_filter_field = "_id"

    userid_list = []
    for user in userid_cursor:
        user_id = user.get("_id")
        userid_list.append(user_id)
        log.info("Object ID received " + str(user_id))

    _filter = {
        mongo_filter_field: {"$in": userid_list},
        "countryCode": {"$in": [91]},
        "docCode": {"$regex": "^ZH"},
        "userFlags.active.activated": {"$eq": True}
    }
    projection = {
        "userId": 1, "_id": 0
    }
    message_replace_data = {}
    action = "dynamic_message"

    try:

        date_string = f'{datetime.now():%Y-%m-%d %H:%M:%S%z}'
        group_id = "broadcast_active_reporting_patients " + date_string

        process_dynamic_message(_filter, projection,
                                message_replace_data, message, action, group_id)
    except Exception as e:
        warning_message = "Query on mongodb unsuccessful"
        log.warning(warning_message)
        log.error(e, exc_info=True)
        raise e
