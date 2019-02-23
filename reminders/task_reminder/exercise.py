from airflow import DAG
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.utils import dates


default_args = {
 'owner': 'airflow',
 'start_date': dates.days_ago(2)
}

dag = DAG(
 dag_id='exercise_reminder',
 default_args=default_args,
 schedule_interval="@once",
)


reminder_7_30 = SimpleHttpOperator(
 task_id="reminder_07_30",
 dag=dag,
 method="POST",
 endpoint="/api/v1/chat/396/message",
 data={},
 headers={},
 log_response=True
)
