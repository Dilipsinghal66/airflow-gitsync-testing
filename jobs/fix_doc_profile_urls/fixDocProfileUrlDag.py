from datetime import datetime, timedelta
from airflow import DAG
from config import default_args, local_tz
from airflow.operators.python_operator import PythonOperator
from jobs.fix_doc_profile_urls.fixDocProfile import fix_doc_profile_url

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(year=2020, month=2, day=3, hour=9, minute=0, second=0, microsecond=0, tzinfo=local_tz),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'fix_doc_profile_dag',
    default_args=default_args,
    description='Dag to sync doc profile url to s3',
    schedule_interval="00 00 * * *",
)

fix_doc_image_task = PythonOperator(
        task_id="fix_doc_profile_url",
        task_concurrency=1,
        python_callable=fix_doc_profile_url,
        dag=dag,
        op_kwargs={},
        pool="scheduled_jobs_pool",
        retry_exponential_backoff=True
)
