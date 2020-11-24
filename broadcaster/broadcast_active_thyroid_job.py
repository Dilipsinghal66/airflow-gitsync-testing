from airflow.models import Variable
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.contrib.hooks.mongo_hook import MongoHook
from pymongo.collection import Collection
from pymongo.cursor import Cursor
from common.db_functions import get_data_from_db
from common.helpers import patient_id_message_send
from datetime import datetime


log = LoggingMixin().log


def get_patient_ids():
    try:
        mongo_conn = MongoHook(conn_id="mongo_prod").get_conn()
        collection = mongo_conn.get_database(
            "tracking").get_collection("md_tracking")
        medicines = ["MED2829", "MED12041", "MED12039", "MED12040", "MED12037", "MED12036", "MED12774", "MED15760", "MED20530", "MED20529",
                     "MED20528", "MED20531", "MED30608", "MED30679", "MED30660", "MED30669", "MED30668", "MED30666", "MED30665", "MED30675"]
        notTimePeriods = ["REFLD197", "REFLD198"]
        results = collection.find(
            {"$and": [{"medicineDetails.medicine": {"$in": medicines}}, {"medicineDetails.timePeriod": {"$nin": notTimePeriods}}]})
        patientIds = []
        for q in results:
            patientIds.append(q['patientId'])

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


def broadcast_active_thyroid():
    patientIds = get_patient_ids()
    print(patientIds)
    process_broadcast_active = int(Variable.get("process_broadcast_active_thyroid",
                                                '1'))
    if process_broadcast_active == 1:
        return

    message = str(Variable.get("broadcast_active_thyroid_msg", ""))
    for patient_id in patientIds:
        if message:
            try:
                patient_id_message_send(patient_id, message, "dynamic_message")
                print(patient_id)
            except Exception as e:
                print("Error Exception raised")
                print(e)
