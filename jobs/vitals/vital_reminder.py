#from sqlalchemy.engine import create_engine
from airflow.models import Variable
from common.db_functions import get_data_from_db
from common.http_functions import make_http_request
from time import sleep
import datetime



def recommendParam():
    ret = ""
    date = datetime.datetime.today()
    timedelta = datetime.timedelta(hours=5, minutes=30)
    todaydate = date + timedelta
    day = todaydate.weekday()

    #elif day == 0:
    #    if param == 5:
    #       ret = 1
    if day == 1:
        ret = "25"
    #elif day == 2:
    #    if param == 5:
    #        ret = 1
    elif day == 3:
        ret = "26"
    #elif day == 4:
    #    if param == 5:
    #        ret = 1
    elif day == 5:
        ret = "68, 69, 71, 10, 18"
    elif day == 6:
        ret = "1"

    return ret

def send_vital_reminder_func():
    try:

        vital_reminder_flag = int(Variable.get("vital_reminder_flag", '0'))
        if vital_reminder_flag == 1:
            return

        payload = {
            "action": "vitals_reminder_6_am",
            "message": "Recommended vitals to test today: \n",
            "is_notification": False,
            "unlock_reporting": True,
            "unlock_vitals": True
        }
        #engine = create_engine('mysql+pymysql://user:user@123@localhost/zylaapi')
        engine = get_data_from_db(db_type="mysql", conn_id="mysql_monolith")
        #print("got db connection from environment")
        connection = engine.get_conn()
        cursor = connection.cursor()
        #print("created connection from engine")

        cursor.execute("select distinct(id) from zylaapi.auth where phoneno in (select phoneno from zylaapi.patient_profile where id in (select distinct(patientId) from zylaapi.patientTestReadings where forDate=CURDATE() and isRecommended = 1))")
        patientIdList = []
        for row in cursor.fetchall():
            for id in row:
                patientIdList.append(id)

        #print(patientIdList)

        recParam = recommendParam()

        recParamMsgSqlQuery = "select name from zylaapi.params where id in (" + recParam + ")"
        number_of_rows = cursor.execute(recParamMsgSqlQuery)

        message = "Recommended vitals to test today: \n"
        if number_of_rows > 0:
            for row in cursor.fetchall():
                for name in row:
                    message = message + name + "\n"

        #print("Hitting recommended jobs end point")
        payload["message"] = "\"" + message + "\""
        print(payload)
        for user_id in patientIdList:
            endpoint = "user/" + str(
                round(user_id)) + "/message"
            #print(endpoint)
            status, body = make_http_request(
                conn_id="http_chat_service_url",
                endpoint=endpoint, method="POST", payload=payload)
            #print(status, body)

            sleep(.300)



    except Exception as e:
        print("Error Exception raised")
        print(e)