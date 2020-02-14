from airflow.models import Variable

from common.helpers import process_dynamic_task_sql


def broadcast_active():

    process_broadcast_active = int(Variable.get("process_broadcast_active",
                                                '0'))
    if process_broadcast_active == 1:
        return

    sql_query = str(Variable.get("broadcast_active_sql_query",
                                 'select id from '
                                 'zylaapi.patient_profile '
                                 'where status = 4'))

    message = str(Variable.get("broadcast_active_msg", ''))
    action = "dynamic_message"
    process_dynamic_task_sql(sql_query, message, action)
     # query = {
     #     "a": {
     #     "$and" :[
     #         {"a": {"$gte": "askd"}},
     #         {"c": "d"}
     #     ]
     # }
