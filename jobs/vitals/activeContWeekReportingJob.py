from airflow.models import Variable
from airflow.utils.log.logging_mixin import LoggingMixin
from datetime import date, timedelta
from common.db_functions import get_data_from_db

log = LoggingMixin().log


def active_week_reporting():

    process_cont_week_active_reporting = int(Variable.get("process_cont_week_"
                                                          "active_reporting_"
                                                          "disable", "0"))
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

    try:

        active_userid_list = get_data_from_db(conn_id='mongo',
                                              collection="user",
                                              filter=user_filter,
                                              projection=user_projection)

        for user in active_userid_list:
            log.debug(str(user))

    except Exception as e:
        warning_message = "First Query on MongoDB unsuccessful"
        log.warning(warning_message)
        log.error(e, exc_info=True)
        raise e

    _filter = {
        {"patientId": {"$in": active_userid_list}},
        {"date": {"$gt": start_date}}
    }

    projection = {
        "patientId": 1
    }
    log.info("--- Second DB call ---")

    try:
        cont_active_userid_list = get_data_from_db(conn_id='mongo',
                                                   collection='report_date',
                                                   filter=_filter,
                                                   projection=projection)
        for userid in cont_active_userid_list:
            log.debug(str(userid))

    except Exception as e:
        warning_message = "Second Query on MongoDB unsuccessful"
        log.warning(warning_message)
        log.error(e, exc_info=True)
        raise e
