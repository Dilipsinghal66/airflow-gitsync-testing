from common.db_functions import get_data_from_db
from airflow.models import Variable
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.contrib.hooks.mongo_hook import MongoHook
from pymongo.collection import Collection
from pymongo.cursor import Cursor


log = LoggingMixin().log

def sendMessage(message, user_id):
    payload = {
        "action": "dynamic_message",
        "message": str(message),
    }
    str(informationIdtobeSent)
    endpoint = "user/" + str(round(user_id)) + "/message"
    status, body = make_http_request(
        conn_id="http_chat_service_url",
        endpoint=endpoint, method="POST", payload=payload)


def getPatientStatus():
    try:        
        engine = get_data_from_db(db_type="mysql", conn_id="mysql_monolith")
        connection = engine.get_conn()
        cursor = connection.cursor()

        cursor.execute("select patient_id,from_status,to_status,updated_on from zylaapi.patient_status_audit")
        patientIds = {}
        for row in cursor.fetchall():
            patientIds[row[0]] = row[2]
            log.info(row)
        
        return patientIds

    except Exception as e:
        log.info("Error Exception raised")
        log.info(e)

def getJourneyMessages():
    try:
        mongo_conn = MongoHook(conn_id="mongo_prod").get_conn()
        collection = mongo_conn.get_database("trialMessageJourney").get_collection("messages")
        results = collection.find({Time: '8:30 AM'})
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
    log.info("Starting...")
    patients = getPatientStatus()
    messages = getJourneyMessages()

    for p in patients:
        log.info("Sending ", p, " message for day ", patients[p], " ", messages[patients[p]])