from time import sleep

from airflow.models import Variable
from airflow.utils.log.logging_mixin import LoggingMixin
from common.db_functions import get_data_from_db
from common.helpers import send_chat_message_patient_id

log = LoggingMixin().log


PAGE_SIZE = 1000


def send_dyn_func():
    try:

        process_dyn_flag = int(Variable.get("process_dyn_flag", '0'))
        if process_dyn_flag == 1:
            return

        engine = get_data_from_db(db_type="mysql", conn_id="mysql_monolith")
        # print("got db connection from environment")
        connection = engine.get_conn()
        cursor = connection.cursor()
        # print("created connection from engine")

        cursor.execute("select count(*) from zylaapi.patient_profile "
                       "where status = 4 and new_chat = 1")
        totalcount = cursor.fetchone()[0]
        # print(totalcount)
        numberofPage = int(totalcount / PAGE_SIZE) + 1
        # print(numberofPage)
        for i in range(numberofPage):
            patientIdSqlQuerry = "select id, countDidYouKnow from " \
                                 "zylaapi.patient_profile where " \
                                 "status = 4  and new_chat = 1 LIMIT " + str(i * PAGE_SIZE) + \
                                 ", " + str(PAGE_SIZE)
            cursor.execute(patientIdSqlQuerry)
            patientIdList = []
            patientIdDict = {}
            for row in cursor.fetchall():
                patientIdList.append(row[0])
                patientIdDict[str(row[0])] = str(row[1])

            print(patientIdDict)

            for key, value in patientIdDict.items():
                informationCardSqlQuery = "select id from " \
                                          "zylaapi.information_cards where " \
                                          "status = 4 and id > " + \
                                          str(value) + " order by id LIMIT 1"
                # print(informationCardSqlQuery)
                number_of_rows = cursor.execute(informationCardSqlQuery)
                # print(number_of_rows)
                if number_of_rows != 0:
                    informationIdtobeSent = cursor.fetchone()[0]
                    log.info("patient_id " + str(key))
                    log.info("Message " + str(informationIdtobeSent))
                    try:
                        payload = {
                            "action": "information_card",
                            "message": str(informationIdtobeSent),
                            "is_notification": False
                        }
                        log.info("Before Message ")
                        #send_chat_message_patient_id(patient_id=int(key), payload=payload)
                    except Exception as e:
                        print("Error Exception raised")
                        print(e)

                    updateSqlQuery = "UPDATE zylaapi.patient_profile SET countDidYouKnow = " + str(  # noqa E303
                            informationIdtobeSent) + " where id = " + str(key)
                    cursor.execute(updateSqlQuery)

                else:
                    print(
                        "All DYN Ids are sent We need to reset this patient Id " + str(  # noqa E303
                            key))
        connection.commit()
    except Exception as e:
        print("Error Exception raised")
        print(e)

