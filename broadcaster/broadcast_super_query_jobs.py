from airflow.models import Variable

from common.helpers import process_custom_message_sql


def broadcast_super_query():

    process_broadcast_super_query = int(Variable.get("process_broadcast_super_query",
                                                '0'))
    if process_broadcast_super_query == 1:
        return

    sql_query = str(Variable.get("broadcast_super_query_sql_query",
                                 'select id from zylaapi.auth where who = \'patient\' and phoneno in (select phoneno '
                                 'from zylaapi.patient_profile where (status = 11 OR status = 5) AND client_code in '
                                 '(\'DC\', \'MG\', \'PY\') AND new_chat = 1 and id in (select user_id from '
                                 'assessment.multi_therapy_answers where answer= 1 and question_id = 1))'))

    message = str(Variable.get("broadcast_super_query_msg", ''))
    process_custom_message_sql(sql_query, message)

