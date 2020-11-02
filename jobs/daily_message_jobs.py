from airflow.models import Variable
from common.db_functions import get_data_from_db
from airflow.utils.log.logging_mixin import LoggingMixin
from common.helpers import patient_id_message_send
import datetime

log = LoggingMixin().log


def daily_message():

    process_broadcast_active = int(Variable.get("process_daily_message",
                                                '0'))
    if process_broadcast_active == 1:
        return

    date = datetime.datetime.today()
    timedelta = datetime.timedelta(hours=5, minutes=30)
    today_date = date + timedelta

    day = today_date.weekday()

    if (day == 0) or (day == 3):
        return

    engine = get_data_from_db(db_type="mysql", conn_id="mysql_monolith")
    connection = engine.get_conn()
    cursor = connection.cursor()
    sql_query = str(Variable.get("daily_message_sql_query", "select id "
                                 "from zylaapi.patient_profile "
                                 "where status = 4 and new_chat = 1 "
                                 "and id not in (select patient_id from "
                                 "zylaapi.patient_status_audit "
                                 "where to_status = 4 and "
                                 "updated_on > DATE_ADD(curdate(), INTERVAL -7 DAY))"))
    cursor.execute(sql_query)
    patient_id_list = []
    for row in cursor.fetchall():
        for _id in row:
            patient_id_list.append(_id)

    for patient_id in patient_id_list:
        message = str(Variable.get("daily_message_msg", ""))
        if message:
            try:
                patient_id_message_send(patient_id, message, "dynamic_message")
            except Exception as e:
                print("Error Exception raised")
                print(e)
