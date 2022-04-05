from airflow.models import Variable
from datetime import datetime

from common.helpers import process_custom_message_sql


def broadcast_pending_renewal():

    process_broadcast_active = int(Variable.get("process_broadcast_pending_renewal",
                                                '0'))
    if process_broadcast_active == 1:
        return

    sql_query = str(Variable.get("broadcast_pending_renewal_sql_query",
                                 "select id from zylaapi.auth where who = \'patient\' and phoneno in (select phoneno "
                                 "from zylaapi.patient_profile where referred_by = 0 and status = 5 AND new_chat = 1)"))

    message = str(Variable.get("broadcast_pending_renewal_msg", ''))

    date_string = f'{datetime.now():%Y-%m-%d %H:%M:%S%z}'
    group_id = "broadcast_pending_renewal " + date_string

    process_custom_message_sql(sql_query, message, group_id)
