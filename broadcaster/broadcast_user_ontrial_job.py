from airflow.models import Variable
from common.db_functions import get_data_from_db
from common.helpers import send_event_request


def broadcast_newuser_ontrial():
    process_broadcast_newuser_ontrial = int(
        Variable.get("process_broadcast_newuser_ontrial", '0'))
    if process_broadcast_newuser_ontrial == 1:
        return

    try:
        engine = get_data_from_db(db_type="mysql", conn_id="mysql_monolith")
        # print("got db connection from environment")
        connection = engine.get_conn()
        # print("got the connection no looking for cursor")
        cursor = connection.cursor()
        # print("got the cursor")

        cursor.execute("Select id,status,created_at,phoneno,countrycode,"
                       "client_code from "
                       "zylaapi.patient_profile where status = 11 and "
                       "TIMESTAMPDIFF(day,created_at,NOW()) = 14 ")

        for row in cursor.fetchall():
            send_event_request(row[0], 20, row[3], row[4], row[5])

    except Exception as e:
        print("Error Exception raised")
        print(e)