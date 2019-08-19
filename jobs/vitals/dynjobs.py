from common.db_functions import get_data_from_db
from common.http_functions import make_http_request
PAGE_SIZE = 1000

def send_dyn():
    try:

        payload = {
            "action": "information_card",
            "message": "String.valueOf(informationCard.getId()",
            "is_notification": True,
            "unlock_reporting": True,
            "unlock_vitals": True
        }

        engine = get_data_from_db(db_type="mysql", conn_id="mysql_monolith")
        print("got db connection from environment")
        connection = engine.get_conn()
        cursor = connection.cursor()
        print("created connection from engine")

        cursor.execute("select count(*) from zylaapi.patient_profile where status in (10,4,11,5,18)")
        totalcount = cursor.fetchone()[0]
        print(totalcount)
        numberofPage = int(totalcount / PAGE_SIZE) + 1
        print(numberofPage)
        for i in range(numberofPage):
            patientIdSqlQuerry = "select id, countDidYouKnow from zylaapi.patient_profile where status in (10,4,11,5,18) LIMIT " + str(i * PAGE_SIZE) + ", " + str(PAGE_SIZE)
            cursor.execute(patientIdSqlQuerry)
            patientIdList = []
            patientIdDict = {}
            for row in cursor.fetchall():
                patientIdList.append(row[0])
                patientIdDict[str(row[0])] = str(row[1]);

            print(patientIdDict)

            for key, value in patientIdDict.items():
                informationCardSqlQuery = "select id from zylaapi.information_cards where status = 4 and id > " + str(
                    value) + " order by id LIMIT 1"
                number_of_rows = cursor.execute(informationCardSqlQuery)
                print(number_of_rows)
                if number_of_rows != 0:
                    informationIdtobeSent = cursor.fetchone()[0]
                    print(informationIdtobeSent)
                    cursor.execute("select distinct(id) from zylaapi.auth where phoneno in (select phoneno from zylaapi.patient_profile where id = " + str(key) + " )")
                    user_id = cursor.fetchone()[0]

                    print(user_id)
                    payload["message"] = str(informationIdtobeSent)
                    endpoint = "user/" + str(round(user_id)) + "/message"
                    print(endpoint)
                    print(payload)
                    status, body = make_http_request(
                        conn_id="http_chat_service_url",
                        endpoint=endpoint, method="POST", payload=payload)
                    print(status, body)

                    updateSqlQuery = "UPDATE zylaapi.patient_profile SET countDidYouKnow = " + str(informationIdtobeSent) + " where id = " + str(key)
                    print(updateSqlQuery)
                    cursor.execute(updateSqlQuery)

                    connection.commit()

                else:
                    print("All DYN Ids are sent We need to reset this patient Id " + str(key))

    except:
        print("Error Exception raised")

send_dyn()