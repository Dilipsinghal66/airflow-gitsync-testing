from airflow.models import Variable

from common.helpers import process_custom_message_sql


def broadcast_inactive():

    process_broadcast_active = int(Variable.get("process_broadcast_inactive",
                                                '1'))
    if process_broadcast_active == 1:
        return

    sql_query = str(Variable.get("broadcast_inactive_jobs_sql_query", "select id from zylaapi.auth where phoneno in "
                                                                      "(select phoneno from patient_profile "
                                                                      "where status in (11, 12, 5, 9) and new_chat=1)"))

    message = str(Variable.get("broadcast_inactive_msg", ''))
    process_custom_message_sql(sql_query, message)

