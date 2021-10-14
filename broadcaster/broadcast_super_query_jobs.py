from airflow.models import Variable

from common.helpers import process_custom_message_sql


def broadcast_super_query():

    process_broadcast_super_query = int(Variable.get("process_broadcast_super_query",
                                                '0'))
    if process_broadcast_super_query == 1:
        return

    sql_query = str(Variable.get("broadcast_super_query_sql_query",
                                 'select id from zylaapi.auth where who = \'patient\' and phoneno in (select '
                                 'phoneno from zylaapi.patient_profile where status = 4 AND '
                                 'new_chat = 1 AND gender = 2)'))

    message = str(Variable.get("broadcast_super_query_msg", ''))
    process_custom_message_sql(sql_query, message)

