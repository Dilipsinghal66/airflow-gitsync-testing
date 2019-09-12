from airflow.contrib.hooks.slack_webhook_hook import SlackWebhookHook


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
