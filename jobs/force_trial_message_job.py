from airflow.models import Variable
from common.db_functions import get_data_from_db
from airflow.utils.log.logging_mixin import LoggingMixin
from common.helpers import patient_id_message_send

log = LoggingMixin().log


def force_trial_message():

    force_trial_message_active = int(Variable.get("force_trial_message_run", '0'))
    if force_trial_message_active == 1:
        return

    engine = get_data_from_db(db_type="mysql", conn_id="mysql_monolith")
    connection = engine.get_conn()
    cursor = connection.cursor()
    sql_query = str(Variable.get("force_trial_message_sql_query", "select id from zylaapi.patient_profile "
                                                                  "where created_at BETWEEN NOW() - INTERVAL 2 HOUR "
                                                                  "AND NOW() - INTERVAL 1 HOUR "
                                                                  "and status not in (4, 11)"))
    cursor.execute(sql_query)
    patient_id_list = []
    for row in cursor.fetchall():
        for _id in row:
            patient_id_list.append(_id)

    for patient_id in patient_id_list:
        message = str(Variable.get("force_trial_message_msg", ""))
        if message:
            try:
                patient_id_message_send(patient_id, message, "start_trial")
            except Exception as e:
                print("Error Exception raised")
                print(e)
