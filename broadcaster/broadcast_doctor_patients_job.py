from airflow.models import Variable
from common.db_functions import get_data_from_db
from common.helpers import process_custom_message_sql


def broadcast_doctor_patients():
    process_broadcast_doctor_patients = int(
        Variable.get("process_broadcast_doctor_patients", '0'))
    if process_broadcast_doctor_patients == 0:
        return

    try:
        engine = get_data_from_db(db_type="mysql", conn_id="mysql_monolith")
        # print("got db connection from environment")
        connection = engine.get_conn()
        # print("got the connection no looking for cursor")
        cursor = connection.cursor()
        # print("got the cursor")

        doc_code = int(
            Variable.get("broadcast_doctor_patients_doccode", 'AB'))
        sql_query = "select id from zylaapi.auth where who = \'patient\' and phoneno in (select phoneno from " \
                    "zylaapi.patient_profile where referred_by = (select id from zylaapi.doc_profile " \
                    "where code = \'" + doc_code.strip() + "\'))"

        message = str(Variable.get("broadcast_doctor_patients_msg", ''))
        process_custom_message_sql(sql_query, message)

    except Exception as e:
        print("Error Exception raised")
        print(e)
