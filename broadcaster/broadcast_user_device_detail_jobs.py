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

        cursor.execute("SELECT p.patient_id,p.phoneno,p.countrycode "
                       "From zylaapi.patient_profile;")

        for row in cursor.fetchall():
            sql_query = "SELECT p.id from zylaapi.auth p INNER JOIN " \
                        "zylaapi.patient_profile q where q.phoneno " \
                        "= p.phoneno and q.countrycode = p.countrycode and " \
                        "p.who = 'patient' and q.id = " + row[0]
            cursor.execute(sql_query)
            user_id = 0
            for row in cursor.fetchall():
                user_id = row[0]
            os = get_user_os_detail(user_id)
            send_user_os_detail_request(row[0], row[1], os, row[2])

    except Exception as e:
        print("Error Exception raised")
        print(e)
