from airflow.models import Variable
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.contrib.hooks.mongo_hook import MongoHook
from pymongo.collection import Collection
from pymongo.cursor import Cursor
from common.db_functions import get_data_from_db
from common.helpers import process_custom_message
from datetime import datetime


log = LoggingMixin().log


def get_patient_ids():
    try:
        mongo_conn = MongoHook(conn_id="mongo_prod").get_conn()
        collection = mongo_conn.get_database(
            "plan-service").get_collection("plan_assignments")
        #plan_ids = [59]

        results = collection.find({"planid": 59})
        patientIds = []

        for q in results:
            patientIds.append(q['patientid'])
        engine = get_data_from_db(db_type="mysql", conn_id="mysql_monolith")
        connection = engine.get_conn()
        cursor = connection.cursor()
        filter_active_patient_query = "select id from zylaapi.auth where who = \'patient\' and phoneno in " \
                                      "(select phoneno from patient_profile where status=4 and new_chat=1 " \
                                      "and id in (" + ','.join(str(x) for x in patientIds) + "))"

        log.info("Checkpoint 3")
        cursor.execute(filter_active_patient_query)
        activePatientIds = []
        for row in cursor.fetchall():
            activePatientIds.append(row[0])

        return activePatientIds
    except Exception as e:
        log.info("Error Exception raised")
        log.info(e)


def broadcast_active_pregnancy():
    user_id_list = get_patient_ids()
    print(user_id_list)
    process_broadcast_active = int(Variable.get("process_broadcast_active_pregnancy", '0'))
    if process_broadcast_active == 1:
        return

    message = str(Variable.get("broadcast_active_pregnancy_msg", ""))

    date_string = f'{datetime.now():%Y-%m-%d %H:%M:%S%z}'
    group_id = "broadcast_active_pregnancy " + date_string

    process_custom_message(user_id_list, message, group_id)
