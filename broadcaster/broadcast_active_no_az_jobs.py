from airflow.models import Variable

from common.helpers import process_dynamic_task_sql_no_az


def broadcast_active_no_az():

    process_broadcast_active = int(Variable.get("process_broadcast_no_az_active",
                                                '0'))
    if process_broadcast_active == 1:
        return

    sql_query = str(Variable.get("broadcast_active_sql_query",
                                 'select id from '
                                 'zylaapi.patient_profile '
                                 'where status = 4 AND new_chat=1'))

    message = str(Variable.get("broadcast_active_no_az_msg", ''))
    action = "dynamic_message"
    process_dynamic_task_sql_no_az(sql_query, message, action)
