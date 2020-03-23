from airflow.models import Variable
from airflow.utils.log.logging_mixin import LoggingMixin
from datetime import timedelta, datetime, date
from common.db_functions import get_data_from_db
from airflow.contrib.hooks.mongo_hook import MongoHook
from airflow.operators.email_operator import EmailOperator
import csv
import json
from common.pyjson import PyJSON

log = LoggingMixin().log


def email_notifier(alerts_default, active_patientid_list):

    email_msg = """
                    <b> Pl. find the list of patients actively reporting <br> 
                    in the duration ({fromDate} to {toDate}) attached with 
                    this mail. 
                    </b> 
                    """.format(fromDate=str(alerts_default.start_date.date()),
                               toDate=str(date.today()))

    subject_msg = "List of active patients continuously reporting for a week"
    files = []

    try:

        if active_patientid_list is not None:

            filename = 'continuousReporting.csv'

            with open(filename, 'w', newline=" ") as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow('PatientId')

                for pid in active_patientid_list:
                    writer.writerow(pid)

            files.append(filename)

        else:
            email_msg = email_msg + "<br><b> No patients were found " \
                                    "reporting continuously for the " \
                                    "given duration </b>"

    except Exception as e:
        warning_message = "Could't make list of patients"
        log.warning(warning_message)
        log.error(e, exc_info=True)
        raise e

    try:

        email_obj = EmailOperator(to=alerts_email,
                                  subject=subject_msg,
                                  html_content=email_msg,
                                  files=files,
                                  mime_charset='utf-8'
                                  )
    except Exception as e:
        warning_message = "Email operator could not be instantiated"
        log.warning(warning_message)
        log.error(e, exc_info=True)
        raise e

    return email_obj.execute(context=None)


def active_week_reporting(**kwargs):

    process_cont_week_active_reporting = int(Variable.get("process_cont_week_"
                                                          "active_reporting_"
                                                          "disable", "0"))
    if process_cont_week_active_reporting:
        return

    delta = 7
    start_date = datetime.now() - timedelta(days=delta)

    user_filter = {
        "userStatus": {"$in": [4]}
    }

    user_projection = {
        "_id": 0,
        "userId": 1,
        "patientId": 1
    }

    try:

        active_userid_cursor = get_data_from_db(conn_id='mongo_user_db',
                                                collection="user",
                                                filter=user_filter,
                                                projection=user_projection)

        active_userid = []
        active_patientid = [1703]

        for user in active_userid_cursor:
            user_id = user.get("userId")
            patient_id = user.get("patientId")
            active_patientid.append(patient_id)
            active_userid.append(user_id)
            log.info("User ID received " + str(user_id))
            log.info("Patient ID received " + str(patient_id))

        log.debug(active_patientid)
        log.debug(type(active_patientid[3]))

    except Exception as e:
        warning_message = "First Query on MongoDB unsuccessful"
        log.warning(warning_message)
        log.error(e, exc_info=True)
        raise e

    log.info("--- Second DB call ---")

    try:

        pipeline = [
            {"$match":
                {
                  "date": {"$gte": start_date}
                  }
             },
            {"$group": {"_id": "$patientId",
                        "count": {"$sum": 1}
                        }
             },
            {"$match": {"count": {"$gte": delta},
                        "_id": {"$in": active_patientid}
                        }
             }
                    ]

        mongo_hook = MongoHook(conn_id='mongo_reporting_db')

        response = mongo_hook.aggregate(mongo_collection='report_date',
                                        aggregate_query=pipeline)

        cont_active_patientid_list = []

        for patient in response:
            patient_id = int(patient.get("_id"))
            count = patient.get("count")
            cont_active_patientid_list.append(patient_id)
            log.debug("Patient ID received " + str(patient_id) + ", count " +
                      str(count))

        email_notifier(alerts_default=alerts_default,
                       active_patientid_list=cont_active_patientid_list)

    except Exception as e:
        warning_message = "Second Query on MongoDB unsuccessful"
        log.warning(warning_message)
        log.error(e, exc_info=True)
        raise e
