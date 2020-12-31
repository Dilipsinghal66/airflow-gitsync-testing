from airflow.models import Variable
from airflow.utils.log.logging_mixin import LoggingMixin
from common.helpers import process_dynamic_task_sql, send_chat_message_patient_id

log = LoggingMixin().log


def broadcast_2020():

    process_broadcast_2020 = int(Variable.get(
        'process_broadcast_2020', '0'))

    if process_broadcast_2020 == 1:
        return

    pids = str(Variable.get("broadcast_2020_id"))
    pids = pids.split(",")
    log.debug(pids)

    for pid in pids:
        try:
            action = "dynamic_message"
            message = str(Variable.get("broadcast_2020_msg", ''))
            message += pid.strip()
            payload = {
                "action": action,
                "message": message,
                "is_notification": False
            }
            send_chat_message_patient_id(int(pid.strip()), payload)
        except Exception as e:
            warning_message = "Cannot send message"
            log.warning(warning_message)
            log.error(e, exc_info=True)
            raise e
