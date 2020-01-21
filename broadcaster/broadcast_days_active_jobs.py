from airflow.models import Variable
import datetime

from common.db_functions import get_data_from_db
from common.helpers import send_chat_message


def broadcast_days_active():

    process_broadcast_days_active = int(Variable.get
                                        ("process_broadcast_days_active", '0'))
    if process_broadcast_days_active == 1:
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
    userid_list = get_data_from_db(conn_id="mongo_user_db",
                                   collection="user_activity",
                                   filter=_filter,
                                   projection=projection)
    mongo_filter_field = "_id"

    _filter = {
        mongo_filter_field: {"$in": userid_list},
        "countryCode": {"$in": [91]},
        "docCode": {"$regex": "^ZH"}
    }
    projection = {
        "userId": 1, "_id": 0
    }
    user_data = get_data_from_db(conn_id="mongo_user_db", collection="user",
                                 filter=_filter, projection=projection)
    payload = {
        "action": "dynamic_message",
        "message": "",
        "is_notification": False
    }
    for user in user_data:
        user_id = user.get("userId")
        payload["message"] = message
        send_chat_message(user_id=user_id, payload=payload)
