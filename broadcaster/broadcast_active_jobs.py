from airflow.models import Variable

from common.helpers import process_custom_message_sql


def broadcast_active():

    process_broadcast_active = int(Variable.get("process_broadcast_active",
                                                '0'))
    if process_broadcast_active == 1:
        return

    sql_query = str(Variable.get("broadcast_active_sql_query", 'select id from zylaapi.auth where phoneno in (select '
                                                               'phoneno from zylaapi.patient_profile where status = 4 '
                                                               'AND new_chat = 1)'))

    message = str(Variable.get("broadcast_active_msg", ''))
    process_custom_message_sql(sql_query, message)

