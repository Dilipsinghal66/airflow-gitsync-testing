from airflow.models import Variable
from common.db_functions import get_data_from_db
from common.helpers import send_event_request_event_name


def broadcast_female_whatsapp():
    process_broadcast_female_whatsapp = int(
        Variable.get("process_broadcast_female_whatsapp", '0'))
    if process_broadcast_female_whatsapp == 0:
        return

    try:
        engine = get_data_from_db(db_type="mysql", conn_id="mysql_monolith")
        # print("got db connection from environment")
        connection = engine.get_conn()
        # print("got the connection no looking for cursor")
        cursor = connection.cursor()
        # print("got the cursor")

        cursor.execute("select id, status, created_at, phoneno, countrycode, client_code from zylaapi.patient_profile "
                       "where gender = 1 and status not in (4, 5) and (referred_by in (select distinct id from "
                       "zylaapi.doc_profile where code like \"ZH%\") or referred_by = 0)")

        for row in cursor.fetchall():
            send_event_request_event_name(row[0], "W-Day", row[3], row[4])

    except Exception as e:
        print("Error Exception raised")
        print(e)
