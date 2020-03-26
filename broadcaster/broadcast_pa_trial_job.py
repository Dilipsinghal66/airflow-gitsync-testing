from airflow.models import Variable
from airflow.utils.log.logging_mixin import LoggingMixin
from datetime import datetime, date, time, timedelta
from common.db_functions import get_data_from_db
from common.helpers import process_dynamic_message
import json
from common.pyjson import PyJSON

log = LoggingMixin().log

"""
Broadcast to patients with lastSeen <= 3 days and having status as ON_TRIAL or
PA_Completed
"""


def broadcast_pa_trial_patients():

    process_broadcast_pa_trial_patients = int(Variable.get
                                              ("process_broadcast_pa_"
                                               "trial_disable", "0"))

    if process_broadcast_pa_trial_patients == 1:
        return

    message = str(Variable.get("broadcast_pa_trial", ''))

    config_var = Variable.get('broadcast_pa_trial_config', None)

    if config_var:
        config_var = json.loads(config_var)
        config_obj = PyJSON(d=config_var)
        db = config_obj.db
        collection = db.collection
        conn_id = db.conn_id
        defaults = config_obj.defaults
        interval = defaults.interval
        status = defaults.status
    else:
        raise ValueError("Config variables not defined")

    start_date = date.today() - timedelta(days=interval)
    start_date_time = datetime.combine(start_date, time.min)

    query_filter = {
        "userStatus": {"$in": status},
        "chatInformation.providerData.lastSeen": {"$gte": start_date_time}
    }

    projection = {
        "_id": 1,
        "userId": 1,
    }

    try:

        mongo_cursor = get_data_from_db(
                                        conn_id=conn_id,
                                        collection=collection,
                                        filter=query_filter,
                                        projection=projection
                                        )
        user_id_list = []
        _id_list = []

        for user in mongo_cursor:
            _id = user.get("_id")
            _id_list.append(_id)
            user_id = user.get("userId")
            user_id_list.append(user_id)
            log.info("_id: " + str(_id) + ", User ID: " + str(user_id))

    except Exception as e:
        warning_message = "Query on MongoDB unsuccessful"
        log.warning(warning_message)
        log.error(e, exc_info=True)
        raise e

    message_replace_data = {}
    action = "dynamic_message"
    _filter = {
        "_id": {"$in": _id_list}
    }

    try:

        process_dynamic_message(_filter=_filter,
                                projection=projection,
                                message_replace_data=message_replace_data,
                                message=message,
                                action=action)

    except Exception as e:
        warning_message = "process_dynamic_message method call unsuccessful"
        log.warning(warning_message)
        log.error(e, exc_info=True)
        raise e
