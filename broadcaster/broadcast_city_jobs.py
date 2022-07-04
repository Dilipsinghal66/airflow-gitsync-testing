from airflow.models import Variable
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.contrib.hooks.mongo_hook import MongoHook
from datetime import datetime
from common.helpers import process_custom_message_sql

log = LoggingMixin().log

def broadcast_city():

    process_broadcast_active = int(Variable.get("process_broadcast_city",
                                                '0'))
    if process_broadcast_active == 1:
        return

    city = str(Variable.get("broadcast_city_var", ''))

    mongo_conn = MongoHook(conn_id="mongo_prod").get_conn()
    collection = mongo_conn.get_database("addressService").get_collection("patient_address")

    results = collection.find({'city': city})
    patientIds = []
    for q in results:
        patientIds.append(q['patientId'])

    filter_active_patient_query = "select id from patient_profile where status = 4 and " \
                                  "new_chat = 1 and id in (" + ','.join(str(x) for x in patientIds) + ")"
    log.info(filter_active_patient_query)

    message = str(Variable.get("broadcast_city_msg", ''))

    date_string = f'{datetime.now():%Y-%m-%d %H:%M:%S%z}'
    group_id = "broadcast_city " + date_string

    process_custom_message_sql(filter_active_patient_query, message, group_id)
