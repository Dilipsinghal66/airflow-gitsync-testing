from airflow.models import Variable

from common.helpers import process_dynamic_task_sql
from datetime import datetime

def broadcast_active_not_hh_cc():

    process_broadcast_active = int(Variable.get("process_broadcast_active_not_hh_cc",
                                                '0'))
    if process_broadcast_active == 1:
        return

    sql_query = str(Variable.get("broadcast_active_not_hh_cc_sql_query",
                                 '''
                                SELECT 
                                    pp.id
                                FROM
                                    (SELECT 
                                        *
                                    FROM
                                        zylaapi.patient_profile
                                    WHERE
                                        status=4 and new_chat=1) pp
                                        LEFT JOIN
                                    (SELECT 
                                        *
                                    FROM
                                        doc_profile
                                    ) dp ON pp.referred_by = dp.id where dp.code is NULL or dp.code like 'AZ%';
                                 '''))

    message = str(Variable.get("broadcast_active_not_hh_cc_msg", ''))
    action = "dynamic_message"

    date_string = f'{datetime.now():%Y-%m-%d %H:%M:%S%z}'
    group_id = "broadcast_active_not_hh_cc " + date_string

    process_dynamic_task_sql(sql_query, message, action, group_id)
