from airflow.models import Variable
from datetime import datetime

from common.helpers import process_custom_message_sql


def broadcast_active_without_doccode():

    process_broadcast_active = int(Variable.get("process_broadcast_active_without_doccode",
                                                '0'))
    if process_broadcast_active == 1:
        return

    sql_query = str(Variable.get("broadcast_active_sql_query",
                                 'select id from zylaapi.auth where phoneno in (select phoneno from '
                                 'zylaapi.patient_profile where status = 4 AND new_chat=1 '
                                 'and client_code != \'AB\' and referred_by '
                                 'in (0, 138602, 20)) and who = \'patient\''))

    message = str(Variable.get("broadcast_active_without_doccode_msg", ''))

    date_string = f'{datetime.now():%Y-%m-%d %H:%M:%S%z}'
    group_id = "broadcast_active_without_doccode " + date_string

    process_custom_message_sql(sql_query, message, group_id)
