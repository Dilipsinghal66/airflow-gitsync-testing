from airflow.models import Variable
from airflow.utils.log.logging_mixin import LoggingMixin
from datetime import date, timedelta
from common.db_functions import get_data_from_db

log = LoggingMixin().log


def active_week_reporting(**kwargs):

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
        "_id": 0,
        "userId": 1
    }

    try:

        active_userid_cursor = get_data_from_db(conn_id='mongo_user_db',
                                                collection="user",
                                                filter=user_filter,
                                                projection=user_projection)

        active_userid = []

        for user in active_userid_cursor:
            user_id = user.get("userId")
            active_userid.append(user_id)
            log.info("User ID received " + str(user_id))

        log.debug(active_userid)

    except Exception as e:
        warning_message = "First Query on MongoDB unsuccessful"
        log.warning(warning_message)
        log.error(e, exc_info=True)
        raise e

    _filter = {
        "patientId": {"$in": active_userid},
        "report_": {"$gt": start_date.isoformat()}
    }

    projection = {
        "patientId": 1
    }
    log.info("--- Second DB call ---")

    try:
        cont_active_userid_cursor = get_data_from_db(conn_id='mongo_user_db',
                                                     collection='report_date',
                                                     filter=_filter,
                                                     projection=projection)

        cont_active_userid_list = []

        for userid in cont_active_userid_cursor:
            user_id = userid.get("userId")
            cont_active_userid_list.append(user_id)
            log.debug("User ID received " + str(user_id))

    except Exception as e:
        warning_message = "Second Query on MongoDB unsuccessful"
        log.warning(warning_message)
        log.error(e, exc_info=True)
        raise e
