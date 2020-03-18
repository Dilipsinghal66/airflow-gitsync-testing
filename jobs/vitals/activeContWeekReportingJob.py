from airflow.models import Variable
from airflow.utils.log.logging_mixin import LoggingMixin
from datetime import date, timedelta
import json
from common.pyjson import PyJSON
from common.db_functions import get_data_from_db

log = LoggingMixin().log


def active_week_reporting():

    process_cont_week_active_reporting = int(Variable.get
                                                  ("process_cont_week_"
                                                   "active_reporting_disable",
                                                   "0"))
    if process_cont_week_active_reporting:
        return

    delta = 7  # airflow var
    start_date = date.today() - timedelta(days=delta)

    user_filter = {
        "userStatus": {"$in": [4]}
    }

    user_projection = {
        "_id": 1
    }

    active_userid_list = get_data_from_db(conn_id='mongo',
                                          collection="user",
                                          filter=user_filter,
                                          projection=user_projection)

    _filter = {
        {"patientId": {"$in": active_userid_list}},
        {"date": {"$gt": start_date}}
    }

    projection = {
        "patientId": 1
    }

    cont_active_userid_list = get_data_from_db(conn_id='mongo',
                                               collection='report_date',
                                               filter=_filter,
                                               projection=projection)















