from airflow.models import Variable
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.contrib.hooks.mongo_hook import MongoHook

from common.db_functions import get_data_from_db
from common.helpers import process_custom_message


log = LoggingMixin().log


def get_user_ids():
    try:
        mongo_conn = MongoHook(conn_id="mongo_prod").get_conn()
        collection = mongo_conn.get_database(
            "plan-service").get_collection("plan_assignments")

        results = collection.find(
            {"planid": {"$in": [31, 42, 44, 48, 49, 51, 53]}})
        patientIds = []
        for q in results:
            patientIds.append(q['patientid'])

        log.info(patientIds)
        engine = get_data_from_db(db_type="mysql", conn_id="mysql_monolith")
        connection = engine.get_conn()
        cursor = connection.cursor()
        userid_query = "select id from zylaapi.auth where phoneno in (select phoneno from " \
            "patient_profile where id in (" + ','.join(str(x) for x in patientIds) \
            + ")) and who = \'patient\'"

        cursor.execute(userid_query)
        user_ids = []
        for row in cursor.fetchall():
            user_ids.append(row[0])

        return user_ids
    except Exception as e:
        log.info("Error Exception raised")
        log.info(e)


def broadcast_covid():
    user_ids = get_user_ids()
    log.info(user_ids)
    process_broadcast_covid = int(Variable.get("process_broadcast_covid", '0'))
    if process_broadcast_covid == 1:
        return

    message = str(Variable.get("broadcast_covid_msg", ""))
    process_custom_message(user_ids, message)
