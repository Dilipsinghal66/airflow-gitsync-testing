from airflow.models import Variable

from common.helpers import process_custom_message_sql
from datetime import datetime

def broadcast_active():

    process_broadcast_active = int(Variable.get("process_broadcast_active",
                                                '0'))
    if process_broadcast_active == 1:
        return

    sql_query = str(Variable.get("broadcast_active_sql_query", 'select id from zylaapi.auth where who = \'patient\' '
                                                               'and phoneno in (select '
                                                               'phoneno from zylaapi.patient_profile where status = 4 '
                                                               'AND new_chat = 1 AND id = 5397)'))

    message = str(Variable.get("broadcast_active_msg", ''))

    date_string = f'{datetime.now():%Y-%m-%d %H:%M:%S%z}'
    group_id = "broadcast_active " + date_string

    process_custom_message_sql(sql_query, message, group_id)

