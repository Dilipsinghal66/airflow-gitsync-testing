from airflow.models import Variable
from common.db_functions import get_data_from_db
from airflow.utils.log.logging_mixin import LoggingMixin
from common.helpers import patient_id_message_send

log = LoggingMixin().log


def force_trial_message():

    force_trial_message_active = int(Variable.get("force_trial_message_run", '0'))
    if force_trial_message_active == 1:
        return

    engine = get_data_from_db(db_type="mysql", conn_id="mysql_monolith")
    connection = engine.get_conn()
    cursor = connection.cursor()
    sql_query = str(Variable.get("force_trial_message_sql_query", "select id, client_code from zylaapi.patient_profile "
                                                                  "where created_at BETWEEN NOW() - INTERVAL 2 HOUR "
                                                                  "AND NOW() - INTERVAL 1 HOUR "
                                                                  "and status not in (4, 11)"))
    cursor.execute(sql_query)
    patient_id_list = []
    patient_id_nv_list = []
    patient_id_cd_list = []
    patient_id_hv_list = []
    patient_id_gp_list = []
    for row in cursor.fetchall():
        if row[1] == "NV":
            patient_id_nv_list.append(row[0])
        elif row[1] == "CD":
            patient_id_cd_list.append(row[0])
        elif row[1] == "HV":
            patient_id_hv_list.append(row[0])
        elif row[1] == "GP":
            patient_id_gp_list.append(row[0])
        else:
            patient_id_list.append(row[0])

    for patient_id in patient_id_list:
        message = str(Variable.get("force_trial_message_msg", ""))
        if message:
            try:
                patient_id_message_send(patient_id, message, "start_trial")
            except Exception as e:
                print("Error Exception raised")
                print(e)
    for patient_id in patient_id_nv_list:
        message = "A warm welcome by Zyla - Your personal healthcare expert. As a part of the wellness benefits " \
                  "extended by Varthana, you are entitled to a <strong>Personal Health Analysis Call</strong>. It is " \
                  "a 20-30 mins call with Zyla’s Medical Team. To initiate your health assessment, " \
                  "please share a suitable time in the chat and our team will schedule the call."
        if message:
            try:
                patient_id_message_send(patient_id, message, "start_trial")
            except Exception as e:
                print("Error Exception raised")
                print(e)
    for patient_id in patient_id_cd_list:
        message = "To book your full body checkup- please click here and submit required details " \
                  "<a href=\"https://zyla-healthcheckup.youcanbook.me\" " \
                  "target=\"_blank\">https://zyla-healthcheckup.youcanbook.me</a>\nIn case of any queries, pls " \
                  "ask here. Always there to help."
        if message:
            try:
                patient_id_message_send(patient_id, message, "start_trial")
            except Exception as e:
                print("Error Exception raised")
                print(e)
    for patient_id in patient_id_hv_list:
        message = "As a part of wellness benefits extended by hyperverge, you are entitled to -\na) <b>Personal " \
                  "Health Analysis Call</b> It is a 20-30 mins call with Zyla Medical Team\nb) <b>Annual Health " \
                  "Check-Up</b> A full body check-up of 61 vitals to keep track of your health organs followed by a " \
                  "Report Analysis call.\nPlease click here and share required details " \
                  "- <a href=\"https://zyla.tiny.us/HRA\" target=\"_blank\">https://zyla.tiny.us/HRA</a>"
        if message:
            try:
                patient_id_message_send(patient_id, message, "start_trial")
            except Exception as e:
                print("Error Exception raised")
                print(e)
    for patient_id in patient_id_gp_list:
        message = "As a part of wellness benefits extended by getpowerplay, you are entitled to -\na) " \
                  "<b>Personal Health Analysis Call</b> It is a 20-30 mins call with Zyla Medical Team\nb) " \
                  "<b>Annual Health Check-Up</b> A full body check-up of 61 vitals to keep track of your health organs " \
                  "followed by a Report Analysis call.\nPlease click here and share required details " \
                  "- <a href=\"https://zyla.tiny.us/HRA\" target=\"_blank\">https://zyla.tiny.us/HRA</a>"
        if message:
            try:
                patient_id_message_send(patient_id, message, "start_trial")
            except Exception as e:
                print("Error Exception raised")
                print(e)
