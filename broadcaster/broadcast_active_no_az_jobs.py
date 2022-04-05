from airflow.models import Variable

from common.helpers import process_custom_message_sql
from datetime import datetime

def broadcast_active_no_az():

    process_broadcast_active = int(Variable.get("process_broadcast_no_az_active",
                                                '0'))
    if process_broadcast_active == 1:
        return

    sql_query = str(Variable.get("broadcast_active_sql_query", 'SELECT p.id from zylaapi.auth p INNER JOIN '
                                                               'zylaapi.patient_profile q where q.phoneno = p.phoneno '
                                                               'and q.countrycode = p.countrycode and p.who = '
                                                               '\'patient\' and (q.referred_by in (select distinct id '
                                                               'from zylaapi.doc_profile where code like \"ZH%\") or '
                                                               'q.referred_by = 0) order by p.id'))

    message = str(Variable.get("broadcast_active_no_az_msg", ''))

    date_string = f'{datetime.now():%Y-%m-%d %H:%M:%S%z}'
    group_id = "broadcast_active_no_az " + date_string

    process_custom_message_sql(sql_query, message, group_id)
