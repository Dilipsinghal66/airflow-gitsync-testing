from airflow.models import Variable
from common.db_functions import get_data_from_db
from common.helpers import get_user_os_detail
from common.helpers import send_user_os_detail_request


def broadcast_user_device_detail():
    process_broadcast_user_device_detail = int(
        Variable.get("process_broadcast_user_device_detail", '0'))
    if process_broadcast_user_device_detail == 1:
        return

    try:
        engine = get_data_from_db(db_type="mysql", conn_id="mysql_monolith")
        # print("got db connection from environment")
        connection = engine.get_conn()
        # print("got the connection no looking for cursor")
        cursor = connection.cursor()
        # print("got the cursor")

        cursor.execute("SELECT p.id,q.id,q.countrycode , q.phoneno from "
                       "zylaapi.auth p INNER JOIN zylaapi.patient_profile q "
                       "where q.phoneno = p.phoneno and q.countrycode = "
                       "p.countrycode and p.who = 'patient';")

        for row in cursor.fetchall():

            os = get_user_os_detail(row[0])
            send_user_os_detail_request(row[1], row[3], os, row[2])

    except Exception as e:
        print("Error Exception raised")
        print(e)
