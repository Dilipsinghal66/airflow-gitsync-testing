from airflow.models import Variable

from common.db_functions import get_data_from_db
from common.helpers import send_chat_message_patient_id
from datetime import date
from common.http_functions import make_http_request
from airflow.utils.log.logging_mixin import LoggingMixin
import dateutil.parser
import datetime

log = LoggingMixin().log


def broadcast_send_pregnancy_card():
    process_broadcast_send_pregnancy_card = int(
        Variable.get("process_broadcast_send_pregnancy_card", '0'))
    if process_broadcast_send_pregnancy_card == 1:
        return
    process_broadcast_pregnancy_card_week_count = int(
        Variable.get("process_broadcast_pregnancy_card_week_count", '0'))

    try:
        log.info(process_broadcast_pregnancy_card_week_count)
        engine = get_data_from_db(db_type="mysql", conn_id="assesment_prod")
        # print("got db connection from environment")
        connection = engine.get_conn()
        # print("got the connection no looking for cursor")
        cursor = connection.cursor()
        # print("got the cursor")

        cursor.execute("SELECT user_id FROM assessment.multi_therapy_answers where question_id=1 and answer='9'")

        msg_id=get_week_msg(connection,process_broadcast_pregnancy_card_week_count)
        for row in cursor.fetchall():
            try:
                log.info(row[0])
                send_msg(row[0], msg_id)
                log.info(week)
            except Exception as e:
                print(e)
        next_week = (process_broadcast_pregnancy_card_week_count+1)%43
        log.info(next_week)
        Variable.set("process_broadcast_pregnancy_card_week_count", next_week)
    except Exception as e:
        print("Error Exception raised")
        print(e)

def send_msg(patient_id,msg_id):
    try:
        payload_dynamic = {
            "action": "custom_message",
            "message": msg_id
        }
        try:
            log.info("msg sent to"+str(patient_id) +" "+msg_id)
        except Exception as e:
            print(e)
            print("Patient "+str(patient_id)+" might not be on new chat")
    except Exception as e:
        print(e)
        print("Patient "+str(patient_id)+" might not be on new chat")

def get_week_msg(connection,week):
    try:
        cursor = connection.cursor()
        cursor.execute("SELECT msg_id FROM assessment.therapy_msg_mapping where week = "+week)
        for row in cursor.fetchall():
            return row[0]

    except Exception as e:
        print("Error Exception raised")
        print(e)