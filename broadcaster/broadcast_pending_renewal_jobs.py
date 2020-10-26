from airflow.models import Variable

from common.helpers import process_dynamic_task_sql


def broadcast_pending_renewal():

    process_broadcast_active = int(Variable.get("process_broadcast_pending_renewal",
                                                '0'))
    if process_broadcast_active == 1:
        return

    sql_query = str(Variable.get("broadcast_pending_renewal_sql_query",
                                 '''
                                 select * from zylaapi.patient_profile where referred_by = 0 and status = 5 and new_chat = 1;
                                 '''))

    message = str(Variable.get("broadcast_pending_renewal_msg", ''))
    action = "dynamic_message"
    process_dynamic_task_sql(sql_query, message, action)
