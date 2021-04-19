from airflow.models import Variable
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.contrib.hooks.mongo_hook import MongoHook

from common.db_functions import get_data_from_db
from common.helpers import patient_id_message_send



log = LoggingMixin().log


def get_patient_ids():
    try:
        mongo_conn = MongoHook(conn_id="mongo_prod").get_conn()
        collection = mongo_conn.get_database("plan-service").get_collection("plan_assignments")

        results = collection.find({"planid":31})
        patientIds = []
        for q in results:
            patientIds.append(q['patientid'])

        log.info(patientIds)
        engine = get_data_from_db(db_type="mysql", conn_id="mysql_monolith")
        connection = engine.get_conn()
        cursor = connection.cursor()
        filter_active_patient_query = "select id from patient_profile where status=4 and new_chat=1 and id in (" + ','.join(
            str(x) for x in patientIds) + ")"

        cursor.execute(filter_active_patient_query)
        activePatientIds = []
        for row in cursor.fetchall():
            activePatientIds.append(row[0])

        return activePatientIds
    except Exception as e:
        log.info("Error Exception raised")
        log.info(e)


def broadcast_covid():
    patientIds = get_patient_ids()
    print(patientIds)
    process_broadcast_covid = int(Variable.get("process_broadcast_covid", '0'))
    if process_broadcast_covid == 1:
        return

    #message = str(Variable.get("broadcast_covid_msg", ""))
    #for patient_id in patientIds:
    #    if message:
    #        try:
    #            patient_id_message_send(patient_id, message, "dynamic_message")
    #        except Exception as e:
    #            print("Error Exception raised")
    #            print(e)
