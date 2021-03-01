from airflow.models import Variable

from common.db_functions import get_data_from_db
from common.helpers import send_event_request


def broadcast_newuser_otp():
    process_broadcast_newuser_otp = int(
        Variable.get("process_broadcast_newuser_otp", '0'))
    if process_broadcast_newuser_otp == 1:
        return

    try:
        engine = get_data_from_db(db_type="mysql", conn_id="mysql_monolith")
        # print("got db connection from environment")
        connection = engine.get_conn()
        # print("got the connection no looking for cursor")
        cursor = connection.cursor()
        # print("got the cursor")

        cursor.execute("Select * from (Select phoneno,countrycode from "
                       "zylaapi.auth where TIMESTAMPDIFF(minute,created_at,"
                       "NOW()) between 15 and 30) as p Where not exists ( "
                       "Select * from zylaapi.patient_profile as q where "
                       "p.phoneno = q.phoneno);")

        for row in cursor.fetchall():
            send_event_request("", 19, row[0], row[1])

    except Exception as e:
        print("Error Exception raised")
        print(e)
