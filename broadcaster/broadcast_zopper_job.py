from airflow.models import Variable
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.contrib.hooks.mongo_hook import MongoHook

from common.helpers import process_custom_message_sql

log = LoggingMixin().log


def broadcast_zooper():

    process_broadcast_zooper = int(Variable.get("process_broadcast_zooper", '0'))
    if process_broadcast_zooper == 1:
        return

    filter_active_patient_query = "select id from patient_profile where status=4 and new_chat=1 and client_code = 'ZP'"
    log.info(filter_active_patient_query)

    message = str(Variable.get("broadcast_zooper_msg", ''))
    #process_custom_message_sql(filter_active_patient_query, message)