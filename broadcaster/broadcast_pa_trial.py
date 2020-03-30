from datetime import datetime
from broadcaster.broadcast_pa_trial_job import broadcast_pa_trial_patients
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from config import default_args, local_tz
from airflow.models import Variable
import json
from common.pyjson import PyJSON

config_var = Variable.get('broadcast_pa_trial_config', None)

if config_var:
    config_var = json.loads(config_var)
    config_obj = PyJSON(d=config_var)
    cron_time = config_obj.defaults.cron_time
else:
    raise ValueError("Config variables not defined")

broadcast_pa_trial_dag = DAG(
    dag_id="broadcast_pa_trial",
    default_args=default_args,
    start_date=datetime(year=2020, month=2, day=3, hour=9, minute=0, second=0,
                        microsecond=0, tzinfo=local_tz),
    schedule_interval=cron_time,
    catchup=False
)

broadcast_pa_trial_task = PythonOperator(
    task_id="broadcast_pa_trial_task",
    task_concurrency=1,
    python_callable=broadcast_pa_trial_patients,
    dag=broadcast_pa_trial_dag,
    op_kwargs={},
    pool="scheduled_jobs_pool",
)
