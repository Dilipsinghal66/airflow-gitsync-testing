from common.db_functions import get_data_from_db
from airflow.models import Variable
from airflow.utils.log.logging_mixin import LoggingMixin
from common.http_functions import make_http_request
from common.helpers import process_custom_message_sql_patient

log = LoggingMixin().log

def broadcast_ova():

    process_broadcast_active = int(Variable.get("process_broadcast_ova",
                                                '0'))
    if process_broadcast_active == 1:
        return

    engine = get_data_from_db(db_type="mysql", conn_id="mysql_monolith")

    connection = engine.get_conn()
    cursor = connection.cursor()

    sql_query = str(Variable.get("broadcast_ova_sql_query", 'select id, phoneno from zylaapi.patient_profile '
                                                            'where status = 4 AND new_chat = 1'))

    message = str(Variable.get("broadcast_ova_msg", ''))

    cursor.execute(sql_query)
    patientIdList = []
    patientIdDict = {}
    for row in cursor.fetchall():
        patientIdList.append(row[0])
        patientIdDict[str(row[0])] = str(row[1])

    patient_phoneno = []
    for key, value in patientIdDict.items():
        try:
            query_endpoint = "/" + str(key) + "/primary"
            query_status, query_data = make_http_request(conn_id="http_pa_url",
                                                         endpoint=query_endpoint, method="GET")
            log.info("Answer got " + query_data["answer"] + "    patient Id   " + str(key))
            if int(query_data["answer"]) == 7:
                patient_phoneno.append(value)
                log.info("patient Id Ova" + str(key))
        except:
            log.error("error for patient Id " + str(key))

    process_custom_message_sql_patient(message, patient_phoneno)
