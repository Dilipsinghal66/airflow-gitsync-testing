from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator

from config import local_tz, default_args
from broadcaster.broadcast_female_whatsapp_jobs import broadcast_female_whatsapp

broadcast_female_whatsapp_cron = str(Variable.get
                                 ("broadcast_female_whatsapp_cron", '@yearly'))

broadcast_female_whatsapp_dag = DAG(
    dag_id="broadcast_female_whatsapp",
    default_args=default_args,
    schedule_interval=broadcast_female_whatsapp_cron,
    catchup=False,
    start_date=datetime(year=2019, month=3, day=31, hour=0, minute=0, second=0,
                        microsecond=0, tzinfo=local_tz),
    dagrun_timeout=timedelta(minutes=50),
)

broadcast_female_whatsapp_task = PythonOperator(
    task_id="broadcast_female_whatsapp",
    task_concurrency=1,
    python_callable=broadcast_female_whatsapp,
    dag=broadcast_female_whatsapp_dag,
    op_kwargs={},
    
    retry_exponential_backoff=True
)
