from common.helpers import send_chat_message_patient_id
from common.pyjson import PyJSON
from airflow.models import Variable
import json
from airflow.utils.log.logging_mixin import LoggingMixin


log = LoggingMixin().log


def send_test_message(**kargs):
    html_test_message = Variable.get('html_test_message_dag_msg', None)
    html_test_patient_id = Variable.get('html_test_patient_id', None)

    if not html_test_message or not html_test_patient_id:
        raise ValueError("Config variables not defined")

    payload = {
        "action": "dynamic_message",
        "message": str(html_test_message),
        "is_notification": False
    }

    try:
        send_chat_message_patient_id(
            patient_id=int(html_test_patient_id), payload=payload)
        log.info("Sending {} to {}".format(
            html_test_message, html_test_patient_id))
    except Exception as e:
        log.info("Error Exception raised")
        log.info(e)
