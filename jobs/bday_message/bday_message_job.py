from airflow.models import Variable
from common.db_functions import get_data_from_db
from airflow.utils.log.logging_mixin import LoggingMixin
from common.helpers import patient_id_message_send
from datetime import datetime

log = LoggingMixin().log


def bday_message():

    process_bday_message = int(Variable.get("process_bday_message",
                                            '0'))
    if process_bday_message == 1:
        return

    engine = get_data_from_db(db_type="mysql", conn_id="mysql_monolith")
    connection = engine.get_conn()
    cursor = connection.cursor()
    sql_query = str(Variable.get("bday_message_sql_query", '''
    SELECT 
        id  
    FROM
        patient_profile
    WHERE
        DATE_FORMAT(dateOfBirth, '%m-%d') = DATE_FORMAT(CURDATE(), '%m-%d') and status=4 and new_chat=1
    '''))
    cursor.execute(sql_query)
    patient_id_list = []
    for row in cursor.fetchall():
        for _id in row:
            patient_id_list.append(_id)

    for patient_id in patient_id_list:
        message = str(Variable.get("bday_message_msg", ""))
        if message:
            try:
                patient_id_message_send(patient_id, message, "custom_message")
                print(patient_id)
            except Exception as e:
                print("Error Exception raised")
                print(e)
