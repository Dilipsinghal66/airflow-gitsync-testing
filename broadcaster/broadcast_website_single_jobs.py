from airflow.models import Variable
from airflow.utils.log.logging_mixin import LoggingMixin
from common.db_functions import get_data_from_db
from common.helpers import fcm_message_send

log = LoggingMixin().log


def broadcast_website_single():
    process_broadcast_website_single = int(
        Variable.get("process_broadcast_website_single", '0'))
    if process_broadcast_website_single == 1:
        return

    try:
        message = str(Variable.get("broadcast_website_single_msg", ''))
        title = str(Variable.get("broadcast_website_single_title", ''))
        firebase_id = str(Variable.get("broadcast_website_single_id", ''))

        firebase_id_list = []
        firebase_id_list.append(firebase_id)
        print(firebase_id_list)

        fcm_message_send(firebase_id_list, message, title)

    except Exception as e:
        print("Error Exception raised")
        print(e)
