from airflow.models import Variable
from common.db_functions import get_data_from_db
from airflow.utils.log.logging_mixin import LoggingMixin
from common.helpers import process_custom_message_user_id, get_medicine_details

from datetime import datetime

log = LoggingMixin().log

def broadcast_active_medicine():

    process_broadcast_active_medicine = int(Variable.get("process_broadcast_active_medicine", '0'))
    if process_broadcast_active_medicine == 1:
        return

    engine = get_data_from_db(db_type="mysql", conn_id="mysql_monolith")
    connection = engine.get_conn()
    cursor = connection.cursor()

    sql_query = str(Variable.get("broadcast_active_medicine_sql_query", 'SELECT id from zylaapi.patient_profile '
                                                                        'where (status = 4 or status = 5) and '
                                                                        '(client_code NOT IN (\'AB\', \'NA\', \'ND\')) AND ' ' id in '
                                                                        '(select patient_id '
                                                                        'from zylaapi.patient_status_audit where (to_'
                                                                        'status = 4 or to_status = 5) and '
                                                                        'created_at < NOW() - INTERVAL 14 DAY)'))
    cursor.execute(sql_query)

    patient_id_list = []
    for row in cursor.fetchall():
        for _id in row:
            patient_id_list.append(_id)

    message = str(Variable.get("broadcast_active_medicine_msg", ''))

    date_string = f'{datetime.now():%Y-%m-%d %H:%M:%S%z}'
    group_id = "broadcast_active_medicine " + date_string

    for patient_id in patient_id_list:
        med_list = get_medicine_details(patient_id)
        log.info(med_list)
        if med_list:
            #msg_str = '<br>'.join(med_list)
            msg_str = "<ul>"
            for med in med_list:
                msg_str = msg_str + "<li>" + med + "</li>"

            msg_str = msg_str + "</ul>"
            if message:
                try:
                    user_id_sql_query = "select id from zylaapi.auth where phoneno = " \
                                        "(select phoneno from zylaapi.patient_profile" \
                                        " where id = " + str(patient_id) +") and countrycode = (select countrycode " \
                                                                          "from zylaapi.patient_profile where id = " + \
                                        str(patient_id) + ") and who = 'patient' "
                    cursor.execute(user_id_sql_query)
                    user_id = 0
                    for row in cursor.fetchall():
                        user_id = row[0]
                    log.info("sending for user id " + str(user_id))
                    if user_id != 0:
                        process_custom_message_user_id(user_id, message, msg_str, group_id)
                except Exception as e:
                    print("Error Exception raised")
                    print(e)


