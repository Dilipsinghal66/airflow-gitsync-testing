from airflow.models import Variable
from datetime import datetime

from common.helpers import process_custom_message_sql


def broadcast_patientids():

    process_broadcast_patientids = int(Variable.get("process_broadcast_patientids",
                                                '0'))
    if process_broadcast_patientids == 1:
        return

    patientIds = Variable.get("broadcast_patientid_array", deserialize_json=True)

    sub_sql_query = 'SELECT phoneno from zylaapi.patient_profile WHERE id IN (' + ','.join(map(str, patientIds)) + ')'

    sql_query = str(Variable.get("broadcast_active_sql_query", 'select id from zylaapi.auth where who = \'patient\' '
                                                               'and phoneno in (' + sub_sql_query + ')'))

    message = str(Variable.get("broadcast_patientids_msg", ''))

    date_string = f'{datetime.now():%Y-%m-%d %H:%M:%S%z}'
    group_id = "broadcast_patientids " + date_string

    process_custom_message_sql(sql_query, message, group_id)

