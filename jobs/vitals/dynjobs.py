from time import sleep

from airflow.models import Variable
from airflow.utils.log.logging_mixin import LoggingMixin
from common.db_functions import get_data_from_db
from common.helpers import send_chat_message_patient_id
from datetime import datetime

log = LoggingMixin().log


PAGE_SIZE = 1000


def send_dyn_func():
    try:

        process_dyn_flag = int(Variable.get("process_dyn_flag", '0'))
        if process_dyn_flag == 1:
            return

        engine1 = get_data_from_db(db_type="mysql", conn_id="mysql_monolith")
        # print("got db connection from environment")
        connection1 = engine1.get_conn()
        cursor1 = connection1.cursor()
        # print("created connection from engine")

        cursor1.execute("select count(*) from zylaapi.patient_profile where client_code != 'AB' and new_chat = 1 "
                        "and status = 4")
        totalcount = cursor1.fetchone()[0]
        # print(totalcount)
        numberofPage = int(totalcount / PAGE_SIZE) + 1
        # print(numberofPage)
        connection1.close()

        date_string = f'{datetime.now():%Y-%m-%d %H:%M:%S%z}'
        group_id = "dyn " + date_string

        for i in range(numberofPage):
            engine = get_data_from_db(db_type="mysql", conn_id="mysql_monolith")
            # print("got db connection from environment")
            connection = engine.get_conn()
            cursor = connection.cursor()
            patientIdSqlQuerry = "select pp.id, pp.countDidYouKnow, pp.phoneno, pp.countrycode, auth.id as userId from " \
                                 "zylaapi.patient_profile pp left join zylaapi.auth auth on " \
                                 " pp.phoneno = auth.phoneno and pp.countrycode = auth.countrycode where " \
                                 " auth.who = 'patient' and pp.status = 4 and " \
                                 " pp.new_chat = 1 and pp.client_code != 'AB' LIMIT " + str(i * PAGE_SIZE) + \
                                 ", " + str(PAGE_SIZE)
            cursor.execute(patientIdSqlQuerry)
            patientIdList = []
            patientIdDict = {}
            patientUserIdDict = {}
            for row in cursor.fetchall():
                patientIdList.append(row[0])
                patientIdDict[str(row[0])] = str(row[1])
                patientUserIdDict[str(row[0])] = str(row[3])

            print(patientIdDict)

            for key, value in patientIdDict.items():

                primaryTherayQuery = "select answer from " \
                                     "assessment.multi_therapy_answers where user_id = " + \
                                     str(patientUserIdDict.get(key)) + " and question_id = 1"
                paresponse_rows = cursor.execute(primaryTherayQuery)
                paresponse = 6
                if paresponse_rows != 0:
                    paresponse = cursor.fetchone()[0]

                informationCardSqlQuery = "select d.id from " \
                                          "dyn_cards.dyn_cards d left join dyn_cards.dyn_primary_therapy_tags dt " \
                                          "on d.id = dt.dyn_card_id where " \
                                          "d.status = 4 and d.id > " + \
                                          str(value) + " and dt.primary_therapy_id in (6," + \
                                          str(paresponse) + ") order by d.id LIMIT 1"
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
                            "is_notification": False,
                            "groupId": group_id
                        }
                        log.info("Before Message ")
                        send_chat_message_patient_id(patient_id=int(key), payload=payload)
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
            connection.close()
    except Exception as e:
        print("Error Exception raised")
        print(e)

