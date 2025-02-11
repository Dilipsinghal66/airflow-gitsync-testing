from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator

from config import local_tz, default_args

s3_cert_bucket = "test"
s3_sync_location = "/tmp/certs/"

certificate_renewal_dag = DAG(
    dag_id="certificateRenewal",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    start_date=datetime(year=2019, month=9, day=13, hour=0, minute=0, second=0,
                        microsecond=0, tzinfo=local_tz),
    dagrun_timeout=timedelta(minutes=50),
)

sync_certificates_from_s3_task = BashOperator(
    task_id="SyncCertsFromS3",
    task_concurrency=1,
    dag=certificate_renewal_dag,
    pool="infra_tasks_pool",
    bash_command="aws s3 sync {} {} && echo ${{return_code}}".format(s3_cert_bucket, s3_sync_location),
)

certbot_renewal_task = ""

update_certificate_acm_task = ""

sync_certificate_to_s3_task = ""
