from airflow.models import Variable
from airflow.utils.log.logging_mixin import LoggingMixin
from common.helpers import process_custom_message_sql

from datetime import datetime

log = LoggingMixin().log

"""
All paid female patients on bridge (status = 4)
"""


def broadcast_active_female_d2c():

    process_broadcast_active_females = int(Variable.get(
        'process_broadcast_active_female_d2c_disable', '0'))

    if process_broadcast_active_females == 1:
        return

    sql_query = str(Variable.get("broadcast_active_female_d2c_sql",
                                 "select id from zylaapi.auth where who = \'patient\' and phoneno in (select phoneno "
                                 "from zylaapi.patient_profile where gender = 1 and status = 4 and referred_by = 0 and "
                                 "new_chat = 1)"))

    message = str(Variable.get("broadcast_active_female_d2c_msg", ''))


    date_string = f'{datetime.now():%Y-%m-%d %H:%M:%S%z}'
    group_id = "broadcast_active_female_d2c " + date_string

    process_custom_message_sql(sql_query, message, group_id)
