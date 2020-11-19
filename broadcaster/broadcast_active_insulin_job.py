from airflow.models import Variable
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.contrib.hooks.mongo_hook import MongoHook
from pymongo.collection import Collection
from pymongo.cursor import Cursor
from common.helpers import send_chat_message_patient_id
from datetime import datetime


log = LoggingMixin().log


def get_patient_ids():
    try:
        mongo_conn = MongoHook(conn_id="mongo_prod").get_conn()
        collection = mongo_conn.get_database(
            "datatable-service").get_collection("md_tracking")
        medicines = ["MED2266", "MED2267", "MED3655", "MED3656", "MED3657", "MED3658", "MED10529", "MED10530", "MED10531", "MED10532",
                     "MED10533", "MED10534", "MED12862", "MED12863", "MED14520", "MED14522", "MED16168", "MED16169", "MED16170", "MED16171"]
        results = collection.find(
            {"medicineDetails.medicine": {"$in": medicines}})
        patientIds = []
        for q in results:
            patientIds.append(q['patientId'])
            log.info(q)

        return patientIds
    except Exception as e:
        log.info("Error Exception raised")
        log.info(e)


def broadcast_active_insulin():
    patientIds = get_patient_ids()
    print(patientIds)
