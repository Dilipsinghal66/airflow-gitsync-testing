from airflow.models import Variable

from common.helpers import process_dynamic_task_sql


def broadcast_active_hh_cc():

    process_broadcast_active = int(Variable.get("process_broadcast_hh_cc_active",
                                                '0'))
    if process_broadcast_active == 1:
        return

    sql_query = str(Variable.get("broadcast_active_hh_cc_sql_query",
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
                                    ) dp ON pp.referred_by = dp.id where dp.code like 'HH%' or dp.code like 'CC%';
                                 '''))

    message = str(Variable.get("broadcast_active_hh_cc_msg", ''))
    action = "dynamic_message"
    process_dynamic_task_sql(sql_query, message, action)
