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
        sql_query = 'SELECT patient_id, sum(value), count(*) FROM vitals.vital_readings where created_at >= ' \
                    'NOW() - INTERVAL 21 DAY and param_id in (5, 25, 27, 58, 66, 67) group by patient_id'
        cursor.execute(sql_query)

        patient_id_list_for_managed = []

        for row in cursor.fetchall():
            if row[2] > 5:
                avg = row[1]//row[2]
                log.info("Avg is  " + avg)
                if 70 <= avg <= 140:
                    patient_id_list_for_managed.append(row[0])

        sql_query = 'select id from zylaapi.patient_profile where param_group_rule_id = 2'
        cursor.execute(sql_query)

        patient_id_list_on_managed = []
        for row in cursor.fetchall():
            patient_id_list_on_managed.append(row[0])

        patient_id_list_for_intense = []
        for patient_id in patient_id_list_on_managed:
            if patient_id not in patient_id_list_for_managed:
                patient_id_list_for_intense.append(patient_id)

        sql_query = 'select zylaapi.patient_profile set param_group_rule_id = 2 where id in ' \
                    '(' ','.join(str(x) for x in patient_id_list_for_managed) + ')'
        #cursor.execute(sql_query)
        log.info("Update query" + sql_query)

        sql_query = 'select zylaapi.patient_profile set param_group_rule_id = 2 where id in ' \
                    '(' ','.join(str(x) for x in patient_id_list_for_intense) + ')'
        #cursor.execute(sql_query)
        log.info("Update query" + sql_query)


    except Exception as e:
        print("Error Exception raised")
        print(e)
