from airflow.models import Variable
from airflow.utils.log.logging_mixin import LoggingMixin
from common.helpers import process_dynamic_task_sql

log = LoggingMixin().log

"""
All paid patients on bridge who have never listened to meditations until today
"""


def broadcast_active_no_med():
    """

    :return:
    """
    process_broadcast_active_no_meditation_disable = int(Variable.get(
        'process_broadcast_active_no_meditation_disable', '0'))

    if process_broadcast_active_no_meditation_disable:
        return

    sql_query_meditation = str(Variable.get("no_meditation",
                                            'SELECT id FROM '
                                            'zylaapi.patient_profile '
                                            'WHERE id NOT IN '
                                            '(SELECT DISTINCT '
                                            'patientId FROM '
                                            'zylaapi.meditationLogs)'))

    try:
        log.debug(sql_query_meditation)

        action = "dynamic_message"
        message = str(Variable.get("broadcast_active_no_meditation_msg",
                                   ''))

        process_dynamic_task_sql(sql_query_meditation, message, action)

    except Exception as e:
        warning_message = "Query on mysql database unsuccessful"
        log.warning(warning_message)
        log.error(e, exc_info=True)
        raise e
