from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator

from config import local_tz, default_args
from broadcaster.broadcast_doctor_patients_job import broadcast_doctor_patients

broadcast_doctor_patients_cron = str(Variable.get
                                 ("broadcast_doctor_patients_cron", '@once'))

broadcast_doctor_patients_dag = DAG(
    dag_id="broadcast_doctor_patients",
    default_args=default_args,
    schedule_interval=broadcast_doctor_patients_cron,
    catchup=False,
    start_date=datetime(year=2019, month=3, day=31, hour=0, minute=0, second=0,
                        microsecond=0, tzinfo=local_tz),
    dagrun_timeout=timedelta(minutes=1),
)

broadcast_doctor_patients_task = PythonOperator(
    task_id="broadcast_doctor_patients",
    task_concurrency=1,
    python_callable=broadcast_doctor_patients,
    dag=broadcast_doctor_patients_dag,
    op_kwargs={},
    
    retry_exponential_backoff=True
)
