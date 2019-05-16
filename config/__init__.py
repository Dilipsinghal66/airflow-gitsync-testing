from datetime import timedelta

import pendulum
import urllib3
from airflow.utils.dates import cron_presets
from urllib3.exceptions import InsecureRequestWarning

urllib3.disable_warnings(InsecureRequestWarning)

cron_time_map = {
    "@every_minute": "* * * * *",
    "@every_5_minutes": "*/5 * * * *",
    "@every_10_minutes": "*/10 * * * *",
    "@every_15_minutes": "*/15 * * * *"
}
cron_presets.update(cron_time_map)

local_tz = pendulum.timezone("Asia/Kolkata")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': 'mrigesh@zyla.in',
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 0,
    'retry_delay': timedelta(minutes=1)
}
