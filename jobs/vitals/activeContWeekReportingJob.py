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


def email_notifier(dest_email, start_date, active_patientid_list, context):

    email_msg = """ Pl. find the list of patients actively reporting <br>
                    in the duration <b>({fromDate} to {toDate})</b> attached
                    with this mail. <br>""".format(
        fromDate=str(start_date.date()),
        toDate=str(date.today()))

    subject_msg = "List of active patients continuously reporting for a week"
    files = []

    try:

        if active_patientid_list.__len__():

            filename = 'continuousReporting.csv'

            with open(filename, 'w') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(['PatientId'])

                for pid in range(len(active_patientid_list)):
                    writer.writerow([active_patientid_list[pid]])

            files.append(filename)

        else:
            email_msg = email_msg + "<br><b>NOTE: No patients were found " \
                                    "reporting continuously for the " \
                                    "given duration </b>"

    except Exception as e:
        warning_message = "Could't make csv file"
        log.warning(warning_message)
        log.error(e, exc_info=True)
        raise e

    try:

        ti = context.get('task_instance')

        email_obj = EmailOperator(to=dest_email,
                                  subject=subject_msg,
                                  html_content=email_msg,
                                  files=files,
                                  mime_charset='utf-8',
                                  task_id=ti.task_id
                                  )
    except Exception as e:
        warning_message = "Email operator could not be instantiated"
        log.warning(warning_message)
        log.error(e, exc_info=True)
        raise e

    return email_obj.execute(context=context)


def active_week_reporting(**context):

    config_var = Variable.get('cont_weekly_reporting_config', None)

    if config_var:
        config_var = json.loads(config_var)
        config_obj = PyJSON(d=config_var)
        if config_obj.disable:
            return
        status_db = config_obj.status_db
        reporting_db = config_obj.reporting_db
        defaults = config_obj.defaults
        interval = defaults.interval
        dest_email = defaults.email

    else:
        raise ValueError("Config variables not defined")

    start_date = datetime.now() - timedelta(days=interval)

    user_filter = {
        "userStatus": {"$in": [4]}
    }

    user_projection = {
        "_id": 0,
        "userId": 1,
        "patientId": 1
    }

    try:

        active_userid_cursor = get_data_from_db(
            conn_id=status_db.conn_id,
            collection=status_db.collection_name,
            filter=user_filter,
            projection=user_projection)

        active_userid = []
        active_patientid = []

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
            {"$match": {"count": {"$gte": interval},
                        "_id": {"$in": active_patientid}
                        }
             }
                    ]

        mongo_hook = MongoHook(conn_id=reporting_db.conn_id)

        response = mongo_hook.aggregate(
            mongo_collection=reporting_db.collection_name,
            aggregate_query=pipeline)

        cont_active_patientid_list = []

        for patient in response:
            patient_id = int(patient.get("_id"))
            count = patient.get("count")
            cont_active_patientid_list.append(patient_id)
            log.debug("Patient ID received " + str(patient_id) + ", count " +
                      str(count))

    except Exception as e:
        warning_message = "Second Query on MongoDB unsuccessful"
        log.warning(warning_message)
        log.error(e, exc_info=True)
        raise e

    email_notifier(dest_email=dest_email,
                   start_date=start_date,
                   active_patientid_list=cont_active_patientid_list,
                   context=context)
