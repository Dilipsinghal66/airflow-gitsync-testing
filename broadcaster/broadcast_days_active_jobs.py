from airflow.models import Variable
import datetime

from common.db_functions import get_data_from_db
from common.helpers import process_dynamic_message


def broadcast_days_active():

    process_broadcast_days_active_disable = int(Variable.get
                                        ("process_broadcast_days_active_disable", '0'))
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
    message_replace_data = {}
    process_dynamic_message(_filter, projection, message_replace_data, message)
