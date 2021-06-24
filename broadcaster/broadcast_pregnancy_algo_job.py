from airflow.models import Variable

from common.db_functions import get_data_from_db
from common.helpers import send_chat_message_patient_id
from datetime import date
import dateutil.parser



def broadcast_send_pregnancy_card():
    process_broadcast_send_pregnancy_card = int(
        Variable.get("process_broadcast_send_pregnancy_card", '0'))
    if process_broadcast_send_pregnancy_card == 1:
        return

    try:
        engine = get_data_from_db(db_type="mysql", conn_id="assesment_prod")
        # print("got db connection from environment")
        connection = engine.get_conn()
        # print("got the connection no looking for cursor")
        icd_week_mapping=get_pregnancy_icds(connection)
        cursor = connection.cursor()
        # print("got the cursor")

        cursor.execute("SELECT user_id FROM assessment.multi_therapy_answers where question_id=1 and answer='9'")

        for row in cursor.fetchall():
            week=get_pregnancy_week(row[0],icd_week_mapping)
            log.info(week)
            # if week!=0:
                # get_week_msg(connection,week)

    except Exception as e:
        print("Error Exception raised")
        print(e)

def get_pregnancy_icds(connection):
    try:
        cursor = connection.cursor()

        cursor.execute("SELECT * FROM assessment.pregnancy_icds")
        icds={}
        result=[]
        for row in cursor.fetchall():
            icds['icd']=row[0]
            icds['week']=row[1]
            result.append(icds)
        return result
    except Exception as e:
        print("Error Exception raised")
        print(e)


def get_pregnancy_week(pid,icds):
    try:
        log.info(pid)
        endpoint = "/dh/" + str(pid) + "/latest"
        log.info(endpoint)
        status, body = make_http_request(
            conn_id="http_tracking_url",
            endpoint=endpoint, method="GET", payload=payload)
        log.info(status)
        for q in body['diagnosisHistory']:
            for i in icds:
                log.info(i)
                log.info(q['diagnosis'])
                if q['diagnosis']==i['icds']:
                    week=i['week']
                    log.info(week)
                    d=dateutil.parser.isoparse(body['dateCreated'])
                    log.info(d)
                    days = abs(date.today()-d).days
                    log.info(days)
                    final_week=week+(days/7)
                    log.info(final_week)
                    return final_week
        return 0
    except Exception as e:
        log.info("unable to get dh for pid "+pid)

# def get_week_msg(conn,week):
#     try:
#         # cursor = connection.cursor()
#         #
#         # cursor.execute("SELECT * FROM assessment.therapy_msg_mapping where week = "+week)
#         # icds={}
#         # result=[]
#         # for row in cursor.fetchall():
#
#
#     except Exception as e:
#         print("Error Exception raised")
#         print(e)