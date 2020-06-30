import datetime

from airflow.models import Variable
from common.db_functions import get_data_from_db
from common.helpers import patient_id_message_send
from airflow.utils.log.logging_mixin import LoggingMixin

log = LoggingMixin().log

meditation_arr = [6667034930268078080, 6667034954884448256, 6667034889881124864,
                  6667034872604786688, 6667034965844164608, 6667034906368933888,
                  6667034811430862848, 6667034858906189824, 6667034826157064192,
                  6667034784109166592]

def meditation_reminder_func():
    try:

        meditation_reminder_disable_flag = int(Variable.get
                                          ("meditation_reminder_disable_flag", '0'))
        if meditation_reminder_disable_flag == 1:
            return

        date = datetime.datetime.today()
        timedelta = datetime.timedelta(hours=5, minutes=30)
        todayDate = date + timedelta

        day = todayDate.weekday()

        if(day == 2) or (day == 6):

            meditation_id = int(Variable.get("meditation_reminder_id", '0'))

            meditation_id = meditation_id % len(meditation_arr)

            Variable.set(key="meditation_reminder_id", value=meditation_id+1)

            engine = get_data_from_db(db_type="mysql", conn_id="mysql_monolith")
            connection = engine.get_conn()
            cursor = connection.cursor()
            cursor.execute("select id from "
                           "zylaapi.patient_profile")
            patient_id_list = []
            for row in cursor.fetchall():
                for _id in row:
                    patient_id_list.append(_id)

            for patient_id in patient_id_list:
                message = str(meditation_arr[meditation_id])

                action = "meditation_reminders"
                log.info("patient_id " + str(patient_id))
                log.info("Message " + message)
                patient_id_message_send(patient_id, message, action)

        else:
            log.info("Skipping as it is not wed or sun")

    except Exception as e:
        print("Error Exception raised")
        print(e)
