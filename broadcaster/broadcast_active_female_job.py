from airflow.models import Variable
from airflow.utils.log.logging_mixin import LoggingMixin
from common.helpers import process_dynamic_task_sql

log = LoggingMixin().log

"""
All paid female patients on bridge (status = 4)
"""


def broadcast_active_fm():

    process_broadcast_active_females = int(Variable.get(
        'process_broadcast_active_females_disable', '0'))

    if process_broadcast_active_females == 1:
        return

    sql_query_female = str(Variable.get("paid_female_patients",
                                        'SELECT id FROM '
                                        'zylaapi.patient_profile '
                                        'WHERE status = 4 AND gender = 1 AND new_chat=1'))

    try:
        log.debug(sql_query_female)

        action = "dynamic_message"
        message = str(Variable.get("broadcast_active_female_msg", ''))

        process_dynamic_task_sql(sql_query_female, message, action)

    except Exception as e:
        warning_message = "Query on mysql database unsuccessful"
        log.warning(warning_message)
        log.error(e, exc_info=True)
        raise e
