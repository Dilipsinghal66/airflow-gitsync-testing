from airflow.models import Variable

from common.helpers import process_custom_message_sql


def broadcast_inactive_two_weeks():

    process_broadcast_inactive_two_weeks = int(Variable.get("process_broadcast_inactive_two_weeks", '1'))
    if process_broadcast_inactive_two_weeks == 1:
        return

    sql_query = str(Variable.get("process_broadcast_inactive_two_weeks_jobs_sql_query", "select id from zylaapi.auth "
                                                                                        "where phoneno in (select "
                                                                                        "phoneno from zylaapi.patient_"
                                                                                        "profile where status not in "
                                                                                        "(4, 5) and new_chat=1 and "
                                                                                        "created_at <= now() - "
                                                                                        "INTERVAL 7 DAY)"))

    message = str(Variable.get("broadcast_inactive_two_weeks_msg", ''))
    process_custom_message_sql(sql_query, message)

