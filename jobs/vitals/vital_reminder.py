x#from sqlalchemy.engine import create_engine
from common.db_functions import get_data_from_db
from common.http_functions import make_http_request

def vital_reminder():
    try:

        payload = {
            "action": "vitals_reminder_6_am",
            "message": "Recommended vitals to test today:\n",
            "is_notification": False,
            "unlock_reporting": True,
            "unlock_vitals": True
        }
        #engine = create_engine('mysql+pymysql://user:user@123@localhost/zylaapi')
        engine = get_data_from_db(db_type="mysql", conn_id="mysql_monolith")
        print("got db connection from environment")
        connection = engine.get_conn()
        cursor = connection.cursor()
        print("created connection from engine")

        cursor.execute("select distinct(id) from zylaapi.auth where phoneno in (select phoneno from zylaapi.patient_profile where id in (select distinct(patientId) from zylaapi.patientTestReadings where forDate=CURDATE() and isRecommended = 1))")
        patientIdList = []
        for row in cursor.fetchall():
            for id in row:
                patientIdList.append(id)

        print(patientIdList)

        print("Hitting DYN jobs end point")
        for user_id in patientIdList:
            endpoint = "user/" + str(
                round(user_id)) + "/message"
            print(endpoint)
            status, body = make_http_request(
                conn_id="http_chat_service_url",
                endpoint=endpoint, method="POST", payload=payload)
            print(status, body)


    except:
        print("Error Exception raised")