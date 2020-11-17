from airflow.models import Variable

from common.helpers import process_dynamic_task_sql


def broadcast_active_b2c_no_1mg():

    process_broadcast_active = int(Variable.get("process_broadcast_active_b2c_no_1mg",
                                                '0'))
    if process_broadcast_active == 1:
        return

    sql_query = str(Variable.get("broadcast_active_b2c_no_1mg_query",
                                 'select id from '
                                 'zylaapi.patient_profile '
                                 'where status = 4 AND new_chat=1 and referred_by=0 AND client_code!="1mg"'))

    message = str(Variable.get("broadcast_active_b2c_no_1mg_msg", ''))
    action = "dynamic_message"
    process_dynamic_task_sql(sql_query, message, action)
