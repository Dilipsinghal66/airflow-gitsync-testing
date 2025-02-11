from airflow.models import Variable
from airflow.utils.log.logging_mixin import LoggingMixin
from common.helpers import process_dynamic_task_sql
from datetime import datetime

log = LoggingMixin().log

"""
All active patients who have not uploaded photos
"""


def broadcast_active_patients_no_profile_pic():

    process_broadcast_active_no_profile_pic = int(Variable.get(
        'process_broadcast_active_no_profile_pic_disable', '0'))

    if process_broadcast_active_no_profile_pic == 1:
        return

    sql_query = str(Variable.get("no_profile_pic_patients",
                                 "SELECT id FROM "
                                 "zylaapi.patient_profile "
                                 "WHERE status = 4 AND profile_image = '' AND new_chat=1"))

    try:
        log.debug(sql_query)

        action = "dynamic_message"
        message = str(Variable.get("broadcast_active_no_profile_pic", ''))

        date_string = f'{datetime.now():%Y-%m-%d %H:%M:%S%z}'
        group_id = "broadcast_active_patients_no_profile_pic " + date_string

        process_dynamic_task_sql(sql_query, message, action, group_id)

    except Exception as e:
        warning_message = "Query on mysql database unsuccessful"
        log.warning(warning_message)
        log.error(e, exc_info=True)
        raise e
