# from sqlalchemy.engine import create_engine
from airflow.models import Variable
from common.db_functions import get_data_from_db
from common.helpers import patient_id_message_send
from airflow.utils.log.logging_mixin import LoggingMixin
import datetime

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
                                     'select id, param_group_rule_id from zylaapi.patient_profile where '
                                     'status = 4 and new_chat = 1'))
        cursor.execute(sql_query)

        patient_id_list = []
        patient_id_dict = {}
        for row in cursor.fetchall():
            patient_id_list.append(row[0])
            patient_id_dict[str(row[0])] = str(row[1])

        sql_query = 'SELECT week FROM vitals.week_switches where param_group_rule_id = 1'
        cursor.execute(sql_query)

        week = 'A'
        for row in cursor.fetchall():
            week = str(row[0])

        log.info("Week is  " + week)

        date = datetime.datetime.today()
        timedelta = datetime.timedelta(hours=5, minutes=30)
        todaydate = date + timedelta
        d1 = todaydate.strftime("%Y%m%d")
        day = todaydate.weekday()
        day = day + 1

        if day == 7:
            day = 0

        log.info("Day is  " + str(day))

        sql_query = 'select distinct param_group_rule_id from zylaapi.patient_profile'
        cursor.execute(sql_query)

        param_grp_id_list = []
        param_grp_id_dict = {}
        for row in cursor.fetchall():
            param_grp_id_list.append(row[0])


        for param_grp_id in param_grp_id_list:

            sql_query = 'SELECT param_id FROM vitals.vital_recommendation_rules where week = \'' + week + '\' ' \
                        'and day = ' + str(day) + ' and param_group_rule_id = ' + str(param_grp_id)
            number_of_rows = cursor.execute(sql_query)

            if number_of_rows > 0:
                param_id_list = []
                for row in cursor.fetchall():
                    param_id_list.append(row[0])

                #message = str(Variable.get("vital_reminder_message", "Recommended vitals to test today: \n"))
                message = ''
                for param_id in param_id_list:
                    param_name_sql_query = "select name from " \
                                           "zylaapi.params " \
                                           "where id = " + str(param_id)
                    cursor.execute(param_name_sql_query)
                    for row1 in cursor.fetchall():
                        for name in row1:
                            message = message + name + "\n"

                param_grp_id_dict[param_grp_id] = message

        log.info("Got param_grp_id_dict")

        sql_query = 'SELECT distinct patient_id FROM vitals.vital_readings where for_date = ' + str(d1)
        cursor.execute(sql_query)
        custom_patient_id_list = []
        custom_patient_id_dict = {}
        for row in cursor.fetchall():
            custom_patient_id_list.append(row[0])

        for patient_id in custom_patient_id_list:
            sql_query = 'SELECT param_id FROM vitals.vital_readings where for_date = ' + str(d1) + ' and patient_id = ' \
                        + str(patient_id)
            cursor.execute(sql_query)
            param_id_list = []
            for row in cursor.fetchall():
                param_id_list.append(row[0])

            message = ''
            for param_id in param_id_list:
                param_name_sql_query = "select name from " \
                                       "zylaapi.params " \
                                       "where id = " + str(param_id)
                cursor.execute(param_name_sql_query)
                for row1 in cursor.fetchall():
                    for name in row1:
                        message = message + name + "\n"

            custom_patient_id_dict[patient_id] = message

        log.info("Got custom patient dict" + str(custom_patient_id_dict))

        pre_message = str(Variable.get("vital_reminder_message", "Recommended vitals to test today: \n"))
        for key, value in patient_id_dict.items():

            log.info("patient id " + str(key))
            if value in param_grp_id_dict:
                if key in custom_patient_id_dict:
                    message_to_send = pre_message + param_grp_id_dict[value] + custom_patient_id_dict[key]
                else:
                    message_to_send = pre_message + param_grp_id_dict[value]
                action = "vitals_reminder_6_am"
                log.info("patient_id " + str(key))
                log.info("Message " + message_to_send)
                #try:
                #    patient_id_message_send(key, message_to_send, action)
                #except Exception as e:
                #    print("Error Exception raised")
                #    print(e)
            if key in custom_patient_id_dict:
                message_to_send = pre_message + custom_patient_id_dict[key]
                action = "vitals_reminder_6_am"
                log.info("patient_id " + str(key))
                log.info("Message " + message_to_send)
                #try:
                #    patient_id_message_send(key, message_to_send, action)
                #except Exception as e:
                #    print("Error Exception raised")
                #    print(e)

    except Exception as e:
        print("Error Exception raised")
        print(e)
