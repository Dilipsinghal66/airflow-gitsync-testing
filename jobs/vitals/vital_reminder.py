# from sqlalchemy.engine import create_engine
from airflow.models import Variable
from common.db_functions import get_data_from_db
from common.helpers import patient_id_message_send
from airflow.utils.log.logging_mixin import LoggingMixin

log = LoggingMixin().log


def send_vital_reminder_func():
    try:

        vital_reminder_disable_flag = int(Variable.get
                                          ("vital_reminder_disable_flag", '0'))
        if vital_reminder_disable_flag == 1:
            return

        engine = get_data_from_db(db_type="mysql", conn_id="mysql_monolith")
        connection = engine.get_conn()
        cursor = connection.cursor()
        sql_query = str(Variable.get("vital_reminder_sql_query",
                                     'select id from '
                                     'zylaapi.patient_profile '
                                     'where status = 4 and new_chat = 1 and id = 5397'))
        cursor.execute(sql_query)
        patient_id_list = []
        for row in cursor.fetchall():
            for _id in row:
                patient_id_list.append(_id)

        for patient_id in patient_id_list:
            message = str(Variable.get("vital_reminder_message", ""))

            param_query = "select distinct(paramId) " \
                          "from zylaapi.patientTestReadings " \
                          "where patientId = " + str(patient_id) +\
                          " and forDate = curdate() and isRecommended = 1;"
            number_of_rows = cursor.execute(param_query)
            if number_of_rows > 0:
                for row in cursor.fetchall():
                    for _id in row:
                        param_name_sql_query = "select name from " \
                                               "zylaapi.params " \
                                               "where id = " + str(_id)
                        cursor.execute(param_name_sql_query)
                        for row1 in cursor.fetchall():
                            for name in row1:
                                message = message + name + "\n"

                action = "vitals_reminder_6_am"
                log.info("patient_id " + str(patient_id))
                log.info("Message " + message)
                try:
                    patient_id_message_send(patient_id, message, action)
                except Exception as e:
                    print("Error Exception raised")
                    print(e)

    except Exception as e:
        print("Error Exception raised")
        print(e)
