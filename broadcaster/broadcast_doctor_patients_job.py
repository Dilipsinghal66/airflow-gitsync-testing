from airflow.models import Variable
from airflow.utils.log.logging_mixin import LoggingMixin
from common.db_functions import get_data_from_db
from common.helpers import process_custom_message_sql
from datetime import datetime

log = LoggingMixin().log

def broadcast_doctor_patients():
    process_broadcast_doctor_patients = int(
        Variable.get("process_broadcast_doctor_patients", '0'))
    if process_broadcast_doctor_patients == 1:
        return

    try:
        doc_code = str(Variable.get("broadcast_doctor_patients_doccode", 'AB'))
        sql_query = "select id from zylaapi.auth where who = \'patient\' and phoneno in (select phoneno from " \
                    "zylaapi.patient_profile where referred_by = (select id from zylaapi.doc_profile " \
                    "where code = \'" + doc_code.strip() + "\'))"

        log.info(sql_query)

        message = str(Variable.get("broadcast_doctor_patients_msg", ''))

        date_string = f'{datetime.now():%Y-%m-%d %H:%M:%S%z}'
        group_id = "broadcast_doctor_patients " + date_string

        process_custom_message_sql(sql_query, message, group_id)

    except Exception as e:
        print("Error Exception raised")
        print(e)
