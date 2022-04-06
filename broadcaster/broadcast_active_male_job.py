from airflow.models import Variable
from airflow.utils.log.logging_mixin import LoggingMixin
from common.helpers import process_dynamic_task_sql
from datetime import datetime

log = LoggingMixin().log

"""
All paid Male patients on bridge (status = 4)
"""


def broadcast_active_male():

    process_broadcast_active_males = int(Variable.get(
        'process_broadcast_active_males_disable', '0'))

    if process_broadcast_active_males == 1:
        return

    sql_query_male = str(Variable.get("paid_male_patients",
                                      'SELECT id FROM '
                                      'zylaapi.patient_profile '
                                      'WHERE status = 4 AND gender = 2 AND new_chat=1'))

    try:
        log.debug(sql_query_male)

        action = "dynamic_message"
        message = str(Variable.get("broadcast_active_male_msg", ''))

        date_string = f'{datetime.now():%Y-%m-%d %H:%M:%S%z}'
        group_id = "broadcast_active_male " + date_string

        process_dynamic_task_sql(sql_query_male, message, action, group_id)

    except Exception as e:
        warning_message = "Query on mysql database unsuccessful"
        log.warning(warning_message)
        log.error(e, exc_info=True)
        raise e
