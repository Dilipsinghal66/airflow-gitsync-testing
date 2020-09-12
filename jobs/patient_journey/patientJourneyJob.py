from common.db_functions import get_data_from_db
from airflow.models import Variable
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.contrib.hooks.mongo_hook import MongoHook
from pymongo.collection import Collection
from pymongo.cursor import Cursor
from common.helpers import send_chat_message_patient_id
from datetime import datetime


log = LoggingMixin().log



def getPatientStatus():
    try:        
        engine = get_data_from_db(db_type="mysql", conn_id="mysql_monolith")
        connection = engine.get_conn()
        cursor = connection.cursor()

        cursor.execute("select patient_id,from_status,to_status,updated_on from zylaapi.patient_status_audit")
        patientIds = {}
        currentDate = datetime.now().date()
        for row in cursor.fetchall():
            patientIds[row[0]] = currentDate - row[3]
            log.info(row)
        
        return patientIds

    except Exception as e:
        log.info("Error Exception raised")
        log.info(e)

def getJourneyMessages(time):
    try:
        mongo_conn = MongoHook(conn_id="mongo_prod").get_conn()
        collection = mongo_conn.get_database("trialMessageJourney").get_collection("messages")
        results = collection.find({'Time': time})
        messages = {}

        print(results)
        for q in results:
            messages[q['Day']] = q['Message']
            log.info(q)
        
        return messages
    except Exception as e:
        log.info("Error Exception raised")
        log.info(e) 

def initializer(**kwargs):
    time = kwargs['time']
    log.info(time)
    log.info("Starting...")
    patients = getPatientStatus()
    messages = getJourneyMessages(time)

    for p in patients:
        payload = {
            "action": "dynamic_message",
            "message": messages[patients[p]],
            "is_notification": False
        }
        #send_chat_message_patient_id(patient_id=int(p), payload=payload)
        log.info("Sending {} day {} message {}".format(p, patients[p], messages[patients[p]]))
