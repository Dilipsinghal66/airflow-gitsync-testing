# from sqlalchemy.engine import create_engine
from airflow.models import Variable
from common.db_functions import get_data_from_db
from common.helpers import patient_id_message_send
from airflow.utils.log.logging_mixin import LoggingMixin
import datetime

log = LoggingMixin().log


def vital_intense_managed():
    try:

        vital_intense_managed_flag = int(Variable.get
                                          ("vital_intense_managed_flag", '0'))
        if vital_intense_managed_flag == 1:
            return

        engine = get_data_from_db(db_type="mysql", conn_id="mysql_monolith")
        connection = engine.get_conn()
        cursor = connection.cursor()

        sql_query = 'SELECT DISTINCT  patient_id FROM vitals.vital_readings where created_at >= ' \
                    'NOW() - INTERVAL 21 DAY and param_id in (5, 25, 27, 58, 66, 67)'
        cursor.execute(sql_query)
        patient_id_distinct = []

        for row in cursor.fetchall():
            patient_id_distinct.append(row[0])

        patient_id_list_for_managed = []

        for patient_id in patient_id_distinct:
            sql_query = 'SELECT value FROM vitals.vital_readings where created_at >= NOW() - INTERVAL 21 DAY  ' \
                        'and param_id in (5, 25, 27, 58, 66, 67) and TRIM(value) is not null  and ' \
                        'TRIM(value) <> \'\' and patient_id = ' + str(patient_id) + ' order by created_at desc LIMIT 5'
            no_0f_rows = cursor.execute(sql_query)
            if no_0f_rows == 5:
                sum = 0
                for row in cursor.fetchall():
                    sum = sum + int(float(row[0]))
                avg = sum//5
                if 70 <= avg <= 140:
                    patient_id_list_for_managed.append(patient_id)

        sql_query = 'select id from zylaapi.patient_profile where param_group_rule_id = 2'
        cursor.execute(sql_query)

        patient_id_list_on_managed = []
        for row in cursor.fetchall():
            patient_id_list_on_managed.append(row[0])

        patient_id_list_for_intense = []
        for patient_id in patient_id_list_on_managed:
            if patient_id not in patient_id_list_for_managed:
                patient_id_list_for_intense.append(patient_id)

        patient_id_list_for_managed_switch = []
        for patient_id in patient_id_list_for_managed:
            if patient_id not in patient_id_list_on_managed:
                patient_id_list_for_managed_switch.append(patient_id)

        log.info("patient_id_list_for_managed " + str(patient_id_list_for_managed))
        log.info("patient_id_list_for_managed " + str(patient_id_list_for_intense))

        if patient_id_list_for_managed:
            sql_query = 'update zylaapi.patient_profile set param_group_rule_id = 2 where id in ' \
                        '(' + ','.join(str(x) for x in patient_id_list_for_managed_switch) + ')'
            cursor.execute(sql_query)
            log.info("Update query" + sql_query)

        if patient_id_list_for_intense:
            sql_query = 'update zylaapi.patient_profile set param_group_rule_id = 1 where id in ' \
                        '(' + ','.join(str(x) for x in patient_id_list_for_intense) + ')'
            cursor.execute(sql_query)
            log.info("Update query" + sql_query)

        connection.commit()

        for patient_id in patient_id_list_for_managed_switch:
            message = 'Congratulations, glad to see that your vitals are keeping under good control. ' \
                      'Hence we will be recommending lesser sugar readings for next few days so that ' \
                      'you have to prick less!'
            try:
                patient_id_message_send(patient_id, message, "dynamic_message")
                print(patient_id)
            except Exception as e:
                print("Error Exception raised")
                print(e)

        for patient_id in patient_id_list_for_intense:
            message = 'Care team has noticed that your average blood sugars are slightly on the higher side, ' \
                      'so we will be requesting more frequent sugar testing next few days!'
            try:
                patient_id_message_send(patient_id, message, "dynamic_message")
                print(patient_id)
            except Exception as e:
                print("Error Exception raised")
                print(e)
    except Exception as e:
        print("Error Exception raised")
        print(e)
