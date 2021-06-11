from airflow.models import Variable
from common.db_functions import get_data_from_db
from common.helpers import send_chat_message_patient_id


def broadcast_remind_webinar():
    process_broadcast_remind_webinar = int(
        Variable.get("process_broadcast_remind_webinar", '0'))
    if process_broadcast_remind_webinar  == 1:
        return

    try:
        engine = get_data_from_db(db_type="mysql", conn_id="webinar_prod")
        # print("got db connection from environment")
        connection = engine.get_conn()
        # print("got the connection no looking for cursor")
        cursor = connection.cursor()
        # print("got the cursor")

        cursor.execute("SELECT id FROM webinar.webinars where timestampdiff(hour,starting_at,NOW())<2")

        for row in cursor.fetchall():
            send_msg_by_webinar_id(row[0])

    except Exception as e:
        print("Error Exception raised")
        print(e)

def send_msg_by_webinar_id(webinar_id):
    try:
        engine = get_data_from_db(db_type="mysql", conn_id="webinar_prod")
        # print("got db connection from environment")
        connection = engine.get_conn()
        # print("got the connection no looking for cursor")
        cursor = connection.cursor()
        # print("got the cursor")
        msg = str(Variable.get("broadcast_remind_webinar_msg", ''))

        cursor.execute("SELECT id,patient_id FROM webinar.app_users where webinar_id = "+webinar_id+" and status = true ;")

        for row in cursor.fetchall():
            payload_dynamic = {
                "action": "dynamic_message",
                "message": msg
            }
            try:
                send_chat_message_patient_id(row[1], payload_dynamic)
            except Exception as e:
                print(e)
                print("Patient"+row[1]+" might not be on new chat")
    except Exception as e:
        print("Error Exception raised")
        print(e)
