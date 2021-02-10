from airflow.models import Variable

from common.helpers import process_dynamic_task_sql


def broadcast_active_ancillary_med():

    process_broadcast_active = int(Variable.get("process_broadcast_active_ancillary_med",
                                                '0'))
    if process_broadcast_active == 1:
        return

    sql_query = str(Variable.get("broadcast_active_ancillary_med_sql_query",
                                 '''
                                SELECT 
                                    pid
                                FROM
                                    (SELECT 
                                        pid, code
                                    FROM
                                        (SELECT 
                                        id AS pid, referred_by
                                    FROM
                                        patient_profile
                                    WHERE
                                        referred_by NOT IN (0 , 68) and new_chat=1 and status=4 )  pp
                                    LEFT JOIN doc_profile dp ON pp.referred_by = dp.id) t
                                        LEFT JOIN
                                    doc_onboarding dob ON t.code = dob.doc_code
                                WHERE
                                    ancillary_med = 'Yes'
                                '''))

    message = str(Variable.get("broadcast_active_ancillary_med_msg", ''))
    action = "dynamic_message"
    process_dynamic_task_sql(sql_query, message, action)
