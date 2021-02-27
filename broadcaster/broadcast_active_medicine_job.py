from airflow.models import Variable
from common.db_functions import get_data_from_db
from airflow.utils.log.logging_mixin import LoggingMixin
from common.helpers import process_custom_message_sql, get_medicine_details

log = LoggingMixin().log

def broadcast_active_medicine():

    process_broadcast_active_medicine = int(Variable.get("process_broadcast_active_medicine", '0'))
    if process_broadcast_active_medicine == 1:
        return

    engine = get_data_from_db(db_type="mysql", conn_id="mysql_monolith")
    connection = engine.get_conn()
    cursor = connection.cursor()

    sql_query = str(Variable.get("broadcast_active_medicine_sql_query", 'select id from zylaapi.patient_profile '
                                                                        'where status = 4 AND new_chat = 1'))
    cursor.execute(sql_query)

    patient_id_list = []
    for row in cursor.fetchall():
        for _id in row:
            patient_id_list.append(_id)

    message = str(Variable.get("broadcast_active_medicine_msg", ''))

    for patient_id in patient_id_list:
        med_list = get_medicine_details(patient_id)
        log.info(med_list)
        if message:
            try:
                log.info("sending for patient id " + str(patient_id))
                #patient_id_message_send(patient_id, message, "start_trial")
            except Exception as e:
                print("Error Exception raised")
                print(e)


    #process_custom_message_sql(sql_query, message)

