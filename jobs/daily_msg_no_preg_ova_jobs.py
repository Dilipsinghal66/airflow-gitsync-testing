from airflow.models import Variable
from common.db_functions import get_data_from_db
from airflow.contrib.hooks.mongo_hook import MongoHook
from airflow.utils.log.logging_mixin import LoggingMixin
from common.helpers import process_custom_message_sql
import datetime

log = LoggingMixin().log

def get_patient_ids():
    try:
        mongo_conn = MongoHook(conn_id="mongo_prod").get_conn()
        collection = mongo_conn.get_database(
            "plan-service").get_collection("plan_assignments")
        plan_ids = [19, 25, 41, 56, 59]

        results = collection.find({"planid": {"$in": plan_ids}})
        patientIds = []

        for q in results:
            patientIds.append(q['patientid'])

        return patientIds
    except Exception as e:
        log.info("Error Exception raised")
        log.info(e)

def daily_msg_no_preg_ova():

    process_daily_msg_no_preg_ova = int(Variable.get("daily_msg_no_preg_ova", '0'))
    if process_daily_msg_no_preg_ova == 1:
        return

    patientIds = get_patient_ids()

    sql_query = str(Variable.get("daily_message_sql_query",
                                 "SELECT id from zylaapi.auth where who = \'patient\' and phoneno in (select phoneno "
                                 "from zylaapi.patient_profile "
                                 "where status = 4 and new_chat = 1 "
                                 "and id not in (select patient_id from "
                                 "zylaapi.patient_status_audit "
                                 "where to_status = 4 and "
                                 "updated_on > DATE_ADD(curdate(), INTERVAL -7 DAY)) and id not "
                                 "in (" + ','.join(str(x) for x in patientIds) + ")" + ")"))
    message = str(Variable.get("daily_message_msg", ""))
    log.info(sql_query)

    date_string = f'{datetime.now():%Y-%m-%d %H:%M:%S%z}'
    group_id = "daily_msg_no_preg_ova " + date_string

    process_custom_message_sql(sql_query, message, group_id)
