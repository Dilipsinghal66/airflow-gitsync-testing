from airflow.models import Variable

from common.helpers import process_custom_message_sql
from datetime import datetime

def broadcast_active_no_az_hh():

    process_broadcast_active = int(Variable.get("process_broadcast_no_az_hh_active",
                                                '0'))
    if process_broadcast_active == 1:
        return

    sql_query = str(Variable.get("broadcast_active_no_az_hh_sql_query", 'select id from zylaapi.auth where who = '
                                                                        '\'patient\' and phoneno in (select phoneno '
                                                                        'from zylaapi.patient_profile where status = 4 '
                                                                        'AND new_chat = 1 and client_code not '
                                                                        'in (\'AZ\', \'HH\'))'))

    message = str(Variable.get("broadcast_active_no_az_hh_msg", ''))

    date_string = f'{datetime.now():%Y-%m-%d %H:%M:%S%z}'
    group_id = "broadcast_active_no_az_hh " + date_string

    process_custom_message_sql(sql_query, message, group_id)
