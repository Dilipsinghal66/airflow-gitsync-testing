from airflow.models import Variable
from common.db_functions import get_data_from_db
from common.helpers import send_event_request, send_notification


def broadcast_single_offer():

    process_broadcast_single_offer = int(Variable.get("process_broadcast_single_offer",
                                                      '0'))
    if process_broadcast_single_offer == 1:
        return

    try:
        send_notification(user_id=4010, title='Test', description='test offer')

    except Exception as e:
        print("Error Exception raised")
        print(e)
