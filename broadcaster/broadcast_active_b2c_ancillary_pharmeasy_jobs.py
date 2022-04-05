from airflow.models import Variable

from common.helpers import process_dynamic_task_sql
from datetime import datetime

def broadcast_active_b2c_ancillary_pharmeasy():

    process_broadcast_active = int(Variable.get("process_broadcast_active_b2c_ancillary_pharmeasy",
                                                '0'))
    if process_broadcast_active == 1:
        return

    sql_query = str(Variable.get("broadcast_active_b2c_ancillary_pharmeasy_query",
                                 'select id from '
                                 'zylaapi.patient_profile '
                                 'where status = 4 AND new_chat=1 AND client_code!="1mg" and (referred_by=0 or referred_by in (20,138602)) '))

    message = str(Variable.get(
        "broadcast_active_b2c_ancillary_pharmeasy_msg", ''))
    action = "dynamic_message"

    date_string = f'{datetime.now():%Y-%m-%d %H:%M:%S%z}'
    group_id = "broadcast_active_b2c_ancillary_pharmeasy " + date_string

    process_dynamic_task_sql(sql_query, message, action, group_id)
