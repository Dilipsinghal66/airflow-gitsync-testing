from airflow.models import Variable

from common.helpers import process_custom_message_sql


def broadcast_inactive_without_cc():

    process_broadcast_active = int(Variable.get("broadcast_inactive_without_cc",
                                                '1'))
    if process_broadcast_active == 1:
        return

    sql_query = str(Variable.get("broadcast_inactive_jobs_sql_query", "select id from zylaapi.auth where phoneno in "
                                                                      "(select phoneno from patient_profile "
                                                                      "where status in (11, 12, 5, 9) and new_chat=1 AND client_code not in ('NV', 'CD', 'HV','GP') )"))

    message = str(Variable.get("broadcast_inactive_without_cc_msg", ''))
    process_custom_message_sql(sql_query, message)

