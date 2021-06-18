from airflow.models import Variable

from common.helpers import process_custom_message_sql


def broadcast_active_ancillary_lab():

    process_broadcast_active = int(Variable.get("process_broadcast_active_ancillary_lab",
                                                '0'))
    if process_broadcast_active == 1:
        return

    sql_query = str(Variable.get("broadcast_active_ancillary_lab_sql_query",
                                 "SELECT id from zylaapi.auth where who = \'patient\' and phoneno in "
                                 "(SELECT t.phoneno FROM (SELECT pid, pp.phoneno, code FROM (SELECT id AS pid, "
                                 "phoneno, referred_by FROM zylaapi.patient_profile WHERE referred_by NOT IN (0 , 68) "
                                 "and new_chat=1 and status=4 )  pp LEFT JOIN zylaapi.doc_profile dp ON "
                                 "pp.referred_by = dp.id) t LEFT JOIN zylaapi.doc_onboarding dob "
                                 "ON t.code = dob.doc_code WHERE ancillary_lab = 'Yes')"))

    message = str(Variable.get("broadcast_active_ancillary_lab_msg", ''))
    process_custom_message_sql(sql_query, message)
