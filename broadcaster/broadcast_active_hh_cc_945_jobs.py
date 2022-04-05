from airflow.models import Variable

from common.helpers import process_custom_message_sql

from datetime import datetime

def broadcast_active_hh_cc_945():

    process_broadcast_active = int(Variable.get("process_broadcast_active_hh_cc_945",
                                                '0'))
    if process_broadcast_active == 1:
        return

    sql_query = str(Variable.get("broadcast_active_hh_cc_945_sql_query", 'SELECT id from zylaapi.auth where phoneno '
                                                                         'in (SELECT pp.phoneno FROM (SELECT * FROM '
                                                                         'zylaapi.patient_profile WHERE status=4 and '
                                                                         'new_chat=1) pp LEFT JOIN (SELECT * FROM '
                                                                         'zylaapi.doc_profile) dp ON pp.referred_'
                                                                         'by = dp.id where dp.code like '
                                                                         '\'HH%\' or dp.code like \'CC%\');'))

    message = str(Variable.get("broadcast_active_hh_cc_945_msg", ''))

    date_string = f'{datetime.now():%Y-%m-%d %H:%M:%S%z}'
    group_id = "broadcast_active_hh_cc_945 " + date_string

    process_custom_message_sql(sql_query, message, group_id)
