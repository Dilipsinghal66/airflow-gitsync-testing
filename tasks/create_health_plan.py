from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from common.helpers import do_level_jump, get_activated_patients
from config import local_tz, default_args
from airflow.models import Variable

activate_patient_duration = int(Variable.get("activated_patient_duration", '0'))

level_jump_dag = DAG(
    dag_id="create_health_plan",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    start_date=datetime(year=2019, month=4, day=23, hour=0, minute=0, second=0, microsecond=0, tzinfo=local_tz)
)

activated_patients_task = PythonOperator(
    task_id="patients_activated",
    task_concurrency=1,
    python_callable=get_activated_patients,
    dag=level_jump_dag,
    op_kwargs={"duration": activate_patient_duration},
    pool="task_reminder_pool",
    retry_exponential_backoff=True
)

level_jump_task = PythonOperator(
    task_id="level_jump_task",
    task_concurrency=1,
    python_callable=do_level_jump,
    dag=level_jump_dag,
    op_kwargs={},
    pool="task_reminder_pool",
    retry_exponential_backoff=True,
    provide_context=True
)

level_jump_task.set_upstream(activated_patients_task)
