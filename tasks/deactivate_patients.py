from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from common.helpers import deactivate_patients
from config import local_tz, default_args

deactivate_patients_dag = DAG(
    dag_id="DeactivatePatients",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    start_date=datetime(year=2019, month=3, day=31, hour=0, minute=0, second=0,
                        microsecond=0, tzinfo=local_tz),
    dagrun_timeout=timedelta(minutes=50),
)

deactivate_patients_task = PythonOperator(
    task_id="deactivatePatients",
    task_concurrency=1,
    python_callable=deactivate_patients,
    dag=deactivate_patients_dag,
    op_kwargs={"userStatus": 5, "assignedCmType": "normal", "assignedCm": 0},
    
    execution_timeout=timedelta(minutes=1),
    on_failure_callback=None
)
