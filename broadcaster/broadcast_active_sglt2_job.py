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
        medicines = ["MED817", "MED818", "MED14464", "MED14463", "MED14465", "MED14462", "MED15245", "MED15246", "MED16868", "MED17264",
                     "MED17263", "MED17261", "MED17262", "MED17260", "MED24126", "MED24128", "MED24124", "MED24125", "MED24127", "MED29433"]
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


def broadcast_active_sglt2():
    patientIds = get_patient_ids()
    print(patientIds)

    process_broadcast_active = int(Variable.get("process_broadcast_active_sglt2",
                                                '1'))
    if process_broadcast_active == 1:
        return

    message = str(Variable.get("broadcast_active_sglt2_msg", ""))

    date_string = f'{datetime.now():%Y-%m-%d %H:%M:%S%z}'
    group_id = "broadcast_active_sglt2 " + date_string

    for patient_id in patientIds:
        if message:
            try:
                patient_id_message_send(patient_id, message, "dynamic_message", group_id)
                print(patient_id)
            except Exception as e:
                print("Error Exception raised")
                print(e)
