from airflow.models import Variable
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.contrib.hooks.mongo_hook import MongoHook
from pymongo.collection import Collection
from pymongo.cursor import Cursor
from common.db_functions import get_data_from_db
from common.helpers import process_custom_message_user_id
from datetime import datetime


log = LoggingMixin().log

def get_medicines():
    try:
        mongo_conn = MongoHook(conn_id="mongo_prod").get_conn()
        collection = mongo_conn.get_database("datatable-service").get_collection("new_medicine")

        results = collection.find({"$or":[{"saltTherapy": {"$elemMatch": {"salt": {"$elemMatch": {"saltName": "ticagrelor"}}}}},
                                          {"saltTherapy": {"$elemMatch": {"salt": {"$elemMatch": {"saltName": "clopidogrel"}}}}}]})
        medicines = []
        for q in results:
            medicines.append(q['mCode'])
        return medicines
    
    except Exception as e:
        log.info("Error Exception raised")
        log.info(e)


def get_patient_ids():
    try:
        mCode = get_medicines()
        print(mCode)

        mongo_conn = MongoHook(conn_id="mongo_prod").get_conn()
        collection = mongo_conn.get_database("tracking").get_collection("md_tracking")
       
        results = collection.find(
            {"medicineDetails.medicine": {"$in": mCode}})
        patientIds = []
        for q in results:
            patientIds.append(q['patientId'])

        engine = get_data_from_db(db_type="mysql", conn_id="mysql_monolith")
        connection = engine.get_conn()
        cursor = connection.cursor()
        filter_active_patient_query = "select id from zylaapi.patient_profile where status=4 and client_code in ('HH','AZ') and new_chat=1 and id in (" + ','.join(
            str(x) for x in patientIds) + ")"

        cursor.execute(filter_active_patient_query)
        activePatientIds = []
        for row in cursor.fetchall():
            activePatientIds.append(row[0])

        return activePatientIds
    except Exception as e:
        log.info("Error Exception raised")
        log.info(e)


def broadcast_active_hh_medicine():
    patientIds = get_patient_ids()
    print(patientIds)
    process_broadcast_active = int(Variable.get("process_broadcast_active_hh_medicine",
                                                '1'))
    if process_broadcast_active == 1:
        return

    date_string = f'{datetime.now():%Y-%m-%d %H:%M:%S%z}'
    group_id = "broadcast_active_hh_medicine" + date_string
    message = str(Variable.get("broadcast_active_hh_medicine_msg", ""))
    engine = get_data_from_db(db_type="mysql", conn_id="mysql_monolith")
    connection = engine.get_conn()
    cursor = connection.cursor()
    for patient_id in patientIds:
        if message:
            try:

                user_id_sql_query = "select id from zylaapi.auth where phoneno = " \
                                    "(select phoneno from zylaapi.patient_profile" \
                                    " where id = " + str(patient_id) + ") and countrycode = (select countrycode " \
                                                                       "from zylaapi.patient_profile where id = " + \
                                    str(patient_id) + ") and who = 'patient' "
                cursor.execute(user_id_sql_query)
                user_id = 0
                for row in cursor.fetchall():
                    user_id = row[0]
                log.info("sending for user id " + str(user_id))
                if user_id != 0:
                    process_custom_message_user_id(user_id, message, "", group_id)
            except Exception as e:
                print("Error Exception raised")
                print(e)