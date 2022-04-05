from airflow.models import Variable
from airflow.utils.log.logging_mixin import LoggingMixin
import datetime

from common.db_functions import get_data_from_db
from common.helpers import process_dynamic_message

log = LoggingMixin().log


def broadcast_days_active():

    process_broadcast_days_active_disable = int(Variable.get
                                                ("process_broadcast_days_"
                                                 "active_disable", '0'))
    if process_broadcast_days_active_disable == 1:
        return

    message = str(Variable.get("broadcast_days_active_msg", ''))

    delta = int(Variable.get("broadcast_days_active_delta", '3'))
    start_date = datetime.datetime.today() - datetime.timedelta(days=delta)
    _filter = {
        "lastActivity": {"$gte": start_date}
    }

    projection = {
        "_id": 1
    }
    userid_cursor = get_data_from_db(conn_id="mongo_user_db",
                                     collection="user_activity",
                                     filter=_filter,
                                     projection=projection)
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

    date_string = f'{datetime.now():%Y-%m-%d %H:%M:%S%z}'
    group_id = "broadcast_days_active " + date_string

    process_dynamic_message(_filter, projection,
                            message_replace_data, message, action, group_id)
