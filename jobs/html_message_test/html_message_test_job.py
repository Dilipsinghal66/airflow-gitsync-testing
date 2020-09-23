from common.helpers import send_chat_message_patient_id
from common.pyjson import PyJSON
from airflow.models import Variable
import json
from airflow.utils.log.logging_mixin import LoggingMixin


log = LoggingMixin().log


def send_test_message(**kargs):
    config_var = Variable.get('html_message_config', None)

    if config_var:
        config_var = json.loads(config_var)
        config_obj = PyJSON(d=config_var)
    else:
        raise ValueError("Config variables not defined")

    payload = {
        "action": "dynamic_message",
        "message": str(config_obj.message),
        "is_notification": False
    }

    try:
        send_chat_message_patient_id(
            patient_id=int(config_obj.patient_id), payload=payload)
        log.info("Sending {} to {}".format(
            config_obj.message, config_obj.patient_id))
    except Exception as e:
        log.info("Error Exception raised")
        log.info(e)
