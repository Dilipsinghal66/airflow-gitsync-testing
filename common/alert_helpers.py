from airflow.contrib.hooks.slack_webhook_hook import SlackWebhookHook
from airflow.operators.email_operator import EmailOperator
from airflow.utils.log.logging_mixin import LoggingMixin

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

    try:

        ti = context.get('task_instance')

    except Exception as e:
        warning_message = "Could'nt get task instance"
        log.warning(warning_message)
        log.error(e, exc_info=True)
        raise e

    email_msg = """
                Task: {task} <br>
                Dag: {dag} <br>
                Execution Time: {exec_date} <br>
                Log Url: {log_url} <br>
                Here is the list of Doctor Codes from AZ doctor spreadsheet
                which have failed schema validation. <br>
                """.format(task=ti.task_id,
                           dag=ti.dag_id,
                           exec_date=context.get('execution_date'),
                           log_url=ti.log_url
                           )

    subject_msg = "List of Doctors failing validation in AZ sync"

    try:

        failed_records = ti.xcom_pull(key='failed_doctor_codes_list')

        failed_records.to_csv(path_or_buf='failed_records.csv', index=False)

    except Exception as e:
        warning_message = "Could't pull failed records list"
        log.warning(warning_message)
        log.error(e, exc_info=True)
        raise e

    try:

        email_obj = EmailOperator(to='mrigesh@zyla.in',
                                  cc='rajat@zyla.in',
                                  subject=subject_msg,
                                  html_content=email_msg,
                                  task_id=ti.task_id,
                                  files=['failed_records.csv']
                                  )
    except Exception as e:
        warning_message = "Email operator could not be instantiated"
        log.warning(warning_message)
        log.error(e, exc_info=True)
        raise e

    return email_obj.execute(context=context)
