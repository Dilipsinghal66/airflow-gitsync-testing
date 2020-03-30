from airflow.models import Variable
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.operators.email_operator import EmailOperator
from common.db_functions import get_data_from_db
import json
from common.pyjson import PyJSON
from datetime import date, datetime, timedelta

log = LoggingMixin().log


def email_notifier(interval, filename, dest_email, context):

    start_date = datetime.now() - timedelta(days=interval)

    email_msg = """ Pl. find the list of patients actively reporting vitals in
    <br> the duration <b>({fromDate} to {toDate})</b> attached with this mail.
    <br> """.format(fromDate=str(start_date.date()), toDate=str(date.today()))

    subject_msg = "List of active patients reporting vitals for a week"
    files = []

    if filename is not None:
        files.append(filename)

    else:
        email_msg = email_msg + "<br><b>NOTE: No patients were found " \
                                "reporting vitals for the given duration </b>"

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


def get_weekly_vitals(**context):

    config_var = Variable.get('weekly_vital_shared_config', None)

    if config_var:
        config_var = json.loads(config_var)
        config_obj = PyJSON(d=config_var)
        if config_obj.disable:
            return
        db = config_obj.db
        target_fields = db.target_fields
        table_name = db.table_name
        defaults = config_obj.defaults
        query_string = defaults.query_string
        interval = defaults.interval
        dest_email = defaults.email

    else:
        raise ValueError("Config variables not defined")

    engine = get_data_from_db(db_type=db.type, conn_id=db.conn_id)

    sql = "SELECT " + target_fields

    if table_name:
        sql = sql + " FROM " + table_name

    if query_string:
        sql = sql + " " + query_string

    log.info(sql)

    try:

        records = engine.get_pandas_df(sql=sql)

    except Exception as e:
        warning_message = "Query on MySQL database unsuccessful"
        log.warning(warning_message)
        log.error(e, exc_info=True)
        raise e

    filename = None

    if not records.empty:

        filename = 'contVitalReporting.csv'

        records.to_csv(path_or_buf=filename, index=False)

    email_notifier(interval=interval, filename=filename, dest_email=dest_email,
                   context=context)
