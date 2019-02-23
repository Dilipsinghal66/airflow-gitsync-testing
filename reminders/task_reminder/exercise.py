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

payload = {
 "action":"tasks_reporting_14_00",
 "is_notification":False
}

headers = {
 "client":"service",
 "access_token":"lKKqOArIvHczgW5w4r9NMF1y41kpXs2v"
}



reminder_7_30 = SimpleHttpOperator(
 task_id="reminder_07_30",
 dag=dag,
 method="POST",
 endpoint="/api/v1/chat/396/message",
 data=payload,
 headers=headers,
 log_response=True,
 http_conn_id="zyla_feature"
)
