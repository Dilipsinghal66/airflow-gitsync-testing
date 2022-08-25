from airflow.models import Variable
from airflow.utils.log.logging_mixin import LoggingMixin
from common.helpers import process_ios_message_sql
from datetime import datetime

log = LoggingMixin().log

"""
All paid Male patients on bridge (status = 4)
"""


def broadcast_ios_user():

    process_broadcast_ios_users = int(Variable.get(
        'process_broadcast_ios_users_disable', '0'))

    if process_broadcast_ios_users == 1:
        return

    sql_query = str(Variable.get("broadcast_active_sql_query", 'select id from zylaapi.auth where who = \'patient\' '
                                                               'and phoneno in (select '
                                                               'phoneno from zylaapi.patient_profile where '
                                                               'new_chat = 1)'))

    try:

        #action = "dynamic_message"
        #message = str(Variable.get("broadcast_ios_user_msg", ''))
        message = "We have rolled out a security update. In case you get logged out, please login again."
        date_string = f'{datetime.now():%Y-%m-%d %H:%M:%S%z}'
        group_id = "broadcast_ios_user " + date_string

        process_ios_message_sql(sql_query, message, group_id)

    except Exception as e:
        warning_message = "Query on mysql database unsuccessful"
        log.warning(warning_message)
        log.error(e, exc_info=True)
        raise e
