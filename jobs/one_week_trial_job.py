from airflow.models import Variable
from datetime import datetime

from common.helpers import process_custom_message_sql


def one_week_trial():

    process_one_week_trial = int(Variable.get("process_one_week_trial", '0'))
    if process_one_week_trial == 1:
        return

    sql_query = str(Variable.get("one_week_trial_sql_query", 'select id from zylaapi.auth where phoneno in (select '
                                                             'phoneno from zylaapi.patient_profile where client_code '
                                                             '= \'ZH\' and created_at <= NOW() - INTERVAL 3 DAY AND '
                                                             'created_at >= NOW() - INTERVAL 4 DAY and status <> 4)'
                                                             ' and who = \'patient\''))

    message = str(Variable.get("one_week_trial_msg", ''))

    date_string = f'{datetime.now():%Y-%m-%d %H:%M:%S%z}'
    group_id = "freemium_journey " + date_string

    process_custom_message_sql(sql_query, message, group_id)
