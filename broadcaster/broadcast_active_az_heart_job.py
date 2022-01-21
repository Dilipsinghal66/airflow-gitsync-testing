from airflow.models import Variable
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.contrib.hooks.mongo_hook import MongoHook

from common.helpers import process_custom_message_sql

log = LoggingMixin().log


def broadcast_active_az_heart():

    process_broadcast_active_az_heart = int(Variable.get("process_broadcast_active_az_heart", '0'))
    if process_broadcast_active_az_heart == 1:
        return

    mongo_conn = MongoHook(conn_id="mongo_prod").get_conn()
    collection = mongo_conn.get_database("plan-service").get_collection("plan_assignments")

    results = collection.find({'planid': 63})
    patientIds = []
    for q in results:
        patientIds.append(q['patientid'])

    filter_active_patient_query = "select id from patient_profile where status = 4 and client_code = 'AZ' and " \
                                  "new_chat = 1 and id in (" + ','.join(str(x) for x in patientIds) + ")"
    log.info(filter_active_patient_query)

    message = str(Variable.get("broadcast_active_az_heart_msg", ''))
    #process_custom_message_sql(filter_active_patient_query, message)