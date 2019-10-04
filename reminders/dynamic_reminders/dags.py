from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator

from common.helpers import get_dynamic_scheduled_message_time
from config import local_tz, default_args
from reminders.dynamic_reminders.reminder import send_dynamic, send_meditation

meditation_schedule = Variable().get(key="meditation_schedule",
                                     deserialize_json=True)

message_times = get_dynamic_scheduled_message_time()
if message_times:
    for message in message_times:
        for k, v in message.items():
            hh, mm = k.split(":")
            time_string = k.replace(":", "_")
            cron_time = mm + " " + hh + " * * *"
            reminder_type = None
            reminder_callable = None
            meditation = False
            if v == 1:
                reminder_type = "reporting"
                reminder_callable = send_dynamic
            elif v == 2:
                reminder_type = "vitals"
                reminder_callable = send_dynamic
            elif v == 3:
                reminder_type = "tasks"
                reminder_callable = send_dynamic
            elif v == 4:
                reminder_type = "dynamic"
                reminder_callable = send_dynamic
                meditation = True
            if not reminder_type:
                continue
            if not reminder_callable:
                continue
            dag_id = reminder_type + "_reminder_" + time_string
            task_id = reminder_type + "_reminder_" + time_string + "_task"
            print(dag_id)
            dag = DAG(
                dag_id=dag_id,
                default_args=default_args,
                schedule_interval=cron_time,
                catchup=False,
                start_date=datetime(year=2019, month=10, day=4, hour=0,
                                    minute=0,
                                    second=0, microsecond=0, tzinfo=local_tz),
                concurrency=1
            )
            print(task_id)
            task = PythonOperator(
                task_id=task_id,
                task_concurrency=1,
                python_callable=reminder_callable,
                dag=dag,
                op_kwargs={"time": k, "reminder_type": v},
                pool="task_reminder_pool",
                retry_exponential_backoff=True,
                provide_context=False

            )
            if meditation:
                meditation_content_21_45_task = PythonOperator(
                    task_id="meditation_content_21_45_task",
                    task_concurrency=1,
                    python_callable=send_meditation,
                    dag=dag,
                    op_kwargs={"schedule": meditation_schedule},
                    pool="task_reminder_pool",
                    retry_exponential_backoff=True,
                    provide_context=True
                )
