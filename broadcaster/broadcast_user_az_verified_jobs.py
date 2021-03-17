from airflow.models import Variable
from common.db_functions import get_data_from_db
from common.helpers import send_event_az_status_request


def broadcast_user_az_verified():
    process_broadcast_user_az_verified = int(
        Variable.get("process_broadcast_user_az_verified", '0'))
    if process_broadcast_user_az_verified == 1:
        return

    try:
        engine = get_data_from_db(db_type="mysql", conn_id="mysql_monolith")
        # print("got db connection from environment")
        connection = engine.get_conn()
        # print("got the connection no looking for cursor")
        cursor = connection.cursor()
        # print("got the cursor")

        cursor.execute("SELECT p.patient_id,p.status,q.phoneno,q.countrycode "
                       "FROM zylaapi.prescription_verification as p INNER "
                       "JOIN zylaapi.patient_profile as q where p.patient_id "
                       "= q.id;")

        for row in cursor.fetchall():
            send_event_az_status_request(row[0], row[1], row[2], row[3])

    except Exception as e:
        print("Error Exception raised")
        print(e)