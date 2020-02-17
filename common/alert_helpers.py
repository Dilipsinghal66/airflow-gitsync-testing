from airflow.contrib.hooks.slack_webhook_hook import SlackWebhookHook
from airflow.operators.email_operator import EmailOperator


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

    ti = context.get('task_instance')

    email_msg = """
                Task: {task}
                Dag: {dag}
                Execution Time: {exec_date}
                Log Url: {log_url}
                Here is the list of Doctor Codes from AZ doctor spreadsheet
                which have failed schema validation.
                """.format(task=ti.task_id,
                           dag=ti.dag_id,
                           exec_date=context.get('execution_date'),
                           log_url=ti.log_url
                           )

    subject_msg = "List of Doctors failing validation in AZ sync"
    failed_records = ti.xcom_pull()

    email_msg.__add__(str(failed_records))

    email_obj = EmailOperator(to='rajat@zyla.in',
                              subject=subject_msg,
                              html_content=email_msg,
                              task_id=ti.task_id
                              )

    return email_obj.execute(context=context)
