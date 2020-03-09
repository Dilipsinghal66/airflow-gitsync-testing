from airflow.contrib.hooks.slack_webhook_hook import SlackWebhookHook
from airflow.operators.email_operator import EmailOperator
from airflow.utils.log.logging_mixin import LoggingMixin
from common.pyjson import PyJSON
from airflow.models import Variable
import json

log = LoggingMixin().log


def task_failure_slack_alert(context):
    slack_msg = """
                :red_circle: Task Failed.
                *Task*: {task}
                *Dag*: {dag}
                *Execution Time*: {exec_date}
                *Log Url*: {log_url}
                """.format(
        task=context.get('task_instance').task_id,
        dag=context.get('task_instance').dag_id,
        ti=context.get('task_instance'),
        exec_date=context.get('execution_date'),
        log_url=context.get('task_instance').log_url,
    )
    failed_alert = SlackWebhookHook(
        http_conn_id='http_slack_url',
        message=slack_msg,
        username='airflow')
    return failed_alert.execute()


def task_success_slack_alert(context):
    pass


def task_failure_email_alert(context):

    config_var = Variable.get('doctor_sync_config', None)

    if config_var:
        config_var = json.loads(config_var)
        config_obj = PyJSON(d=config_var)
        alerts = config_obj.alerts
        alerts_email = alerts.email
    else:
        raise ValueError("Config variables not defined")

    try:

        ti = context.get('task_instance')

    except Exception as e:
        warning_message = "Could'nt get task instance"
        log.warning(warning_message)
        log.error(e, exc_info=True)
        raise e

    email_msg = """
                <h3> Task Failed </h3>
                <b> Task: </b> {task} <br>
                <b> Dag: </b> {dag} <br>
                <b> Execution Time: </b> {exec_date} <br>
                <b> Logs: </b> {log_url} <br>
                """.format(task=ti.task_id,
                           dag=ti.dag_id,
                           exec_date=context.get('execution_date'),
                           log_url=ti.log_url
                           )

    subject_msg = "[ALERT] Task Failed! Task Id: " + ti.task_id
    files = []

    try:

        failed_records_report = ti.xcom_pull(key=context.get('key'))

        if failed_records_report is not None:

            report_name = 'failure_report_taskid_' + ti.task_id + '.csv'

            failed_records_report.to_csv(path_or_buf=report_name, index=False)

            files.append(report_name)

    except Exception as e:
        warning_message = "Could't pull failed records list"
        log.warning(warning_message)
        log.error(e, exc_info=True)

    try:

        email_obj = EmailOperator(to=alerts_email,
                                  subject=subject_msg,
                                  html_content=email_msg,
                                  task_id=ti.task_id,
                                  files=files,
                                  mime_charset='utf-8'
                                  )
    except Exception as e:
        warning_message = "Email operator could not be instantiated"
        log.warning(warning_message)
        log.error(e, exc_info=True)
        raise e

    return email_obj.execute(context=context)
