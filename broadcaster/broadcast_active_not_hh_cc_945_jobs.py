from airflow.models import Variable

from common.helpers import process_custom_message_sql


def broadcast_active_not_hh_cc_945():

    process_broadcast_active = int(Variable.get("process_broadcast_active_not_hh_cc_945",
                                                '0'))
    if process_broadcast_active == 1:
        return

    sql_query = str(Variable.get("broadcast_active_not_hh_cc_sql_query", 'SELECT id from zylaapi.auth where phoneno '
                                                                         'in (SELECT pp.id FROM (SELECT * FROM '
                                                                         'zylaapi.patient_profile WHERE status=4 '
                                                                         'and new_chat=1) pp LEFT JOIN '
                                                                         '(SELECT * FROM zylaapi.doc_profile) '
                                                                         'dp ON pp.referred_by = dp.id where dp.code '
                                                                         'is NULL or dp.code like \'AZ%\');'))

    message = str(Variable.get("broadcast_active_not_hh_cc_945_msg", ''))
    process_custom_message_sql(sql_query, message)
