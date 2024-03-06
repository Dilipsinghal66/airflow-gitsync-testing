from airflow.models import Variable
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.contrib.hooks.mongo_hook import MongoHook

from common.db_functions import get_data_from_db
from common.helpers import process_custom_message
from datetime import datetime


log = LoggingMixin().log


def get_user_ids():
    try:
        mongo_conn = MongoHook(conn_id="mongo_prod").get_conn()
        collection = mongo_conn.get_database(
            "tracking").get_collection("dh_tracking")
        icds = ['ICD26272', 'ICD3895', 'ICD3956']
        results = collection.find(
            {"diagnosisHistory.diagnosis": {"$in": icds}})
        patientIds = []
        for q in results:
            patientIds.append(q['patientId'])

        engine = get_data_from_db(db_type="mysql", conn_id="mysql_monolith")
        connection = engine.get_conn()
        cursor = connection.cursor()
        filter_active_patient_query = "select id from zylaapi.auth where who = \'patient\' and " \
                                      "phoneno in (select phoneno from patient_profile where status=4 and new_chat=1 and client_code in ('DC', 'MG', 'PY') " \
                                      "and id in (" + ','.join(str(x) for x in patientIds) + "))"

        cursor.execute(filter_active_patient_query)
        activePatientIds = []
        for row in cursor.fetchall():
            activePatientIds.append(row[0])

        return activePatientIds
    except Exception as e:
        log.info("Error Exception raised")
        log.info(e)


def broadcast_active_diabetes_d2c():
    user_id_list = get_user_ids()
    print(user_id_list)
    print("oko")
    process_broadcast_active = int(Variable.get("process_broadcast_active_diabetes_d2c",
                                                '1'))
    if process_broadcast_active == 1:
        return

    date_string = f'{datetime.now():%Y-%m-%d %H:%M:%S%z}'
    group_id = "broadcast_active_az " + date_string

    message = str(Variable.get("broadcast_active_diabetes_d2c_msg", ""))
    process_custom_message(user_id_list, message, group_id)
