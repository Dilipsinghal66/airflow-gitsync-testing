from airflow.models import Variable
from common.db_functions import get_data_from_db
from airflow.utils.log.logging_mixin import LoggingMixin
from common.helpers import process_custom_message_sql
from datetime import datetime

log = LoggingMixin().log


def daily_message():

    process_broadcast_active = int(Variable.get("process_daily_message",
                                                '0'))
    if process_broadcast_active == 1:
        return

    sql_query = str(Variable.get("daily_message_sql_query",
                                 "SELECT id from zylaapi.auth where who = \'patient\' and phoneno in (select phoneno "
                                 "from zylaapi.patient_profile "
                                 "where status = 4 and client_code != \'AB\' and new_chat = 1 "
                                 "and id not in (select patient_id from "
                                 "zylaapi.patient_status_audit "
                                 "where to_status = 4 and "
                                 "updated_on > DATE_ADD(curdate(), INTERVAL -7 DAY)))"))
    message = str(Variable.get("daily_message_msg", ""))

    date_string = f'{datetime.now():%Y-%m-%d %H:%M:%S%z}'
    group_id = "daily_message " + date_string

    process_custom_message_sql(sql_query, message, group_id)


