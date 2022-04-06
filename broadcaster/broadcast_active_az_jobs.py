from airflow.models import Variable

from common.helpers import process_dynamic_task_sql

from datetime import datetime
def broadcast_active_az():

    process_broadcast_active = int(Variable.get("process_broadcast_az_active",
                                                '0'))
    if process_broadcast_active == 1:
        return

    sql_query = str(Variable.get("broadcast_active_sql_query",
                                 'select id from '
                                 'zylaapi.patient_profile '
                                 'where status = 4 AND new_chat=1 AND referred_by!=0 and referred_by in '
                                 '(select id from zylaapi.doc_profile where code like "AZ%")'))

    message = str(Variable.get("broadcast_active_az_msg", ''))
    action = "dynamic_message"

    date_string = f'{datetime.now():%Y-%m-%d %H:%M:%S%z}'
    group_id = "broadcast_active_az " + date_string

    process_dynamic_task_sql(sql_query, message, action, group_id)
