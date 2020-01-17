from airflow.models import Variable
from airflow.utils.log.logging_mixin import LoggingMixin

from common.db_functions import get_data_from_db
from common.helpers import send_chat_message

PAGE_SIZE = 1000
log = LoggingMixin().log


def active_patients():

    mongo_filter_field = "patientId"
    process_active_patients = int(Variable.get("process_active_patients", '0'))
    if process_active_patients == 1:
        return

    payload = {
        "action": "dynamic_message",
        "message": "",
        "is_notification": False
    }

    sql_query = str(Variable.get("active_patients_sql_query",
                                 'select id from '
                                 'patient_profile where status = 4'))

    message = str(Variable.get("active_patients_msg", ''))

    sql_data = get_data_from_db(db_type="mysql", conn_id="mysql_monolith",
                                sql_query=sql_query, execute_query=True)
    patient_id_list = []
    message_replace_data = {}

    for patient in sql_data:
        patient_id = patient[0]
        patient_id_list.append(patient_id)
        message_replace_data[patient_id] = patient

    log.info(patient_id_list)

    _filter = {
        mongo_filter_field: {"$in": patient_id_list},
        "countryCode": {"$in": [91]},
        "docCode": {"$regex": "^ZH"}
    }
    projection = {
        "userId": 1, "patientId": 1, "_id": 0
    }
    user_data = get_data_from_db(conn_id="mongo_user_db", collection="user",
                                 filter=_filter, projection=projection)

    for user in user_data:
        user_id = user.get("userId")
        patient_id = user.get("patientId")
        patient_data = message_replace_data.get(patient_id)
        for i in range(0, len(patient_data)):
            old = "#" + str(i) + "#"
            new = str(patient_data[i])
            patient_message = message.replace(old, new)
        payload["message"] = patient_message
        send_chat_message(user_id=user_id, payload=payload)
