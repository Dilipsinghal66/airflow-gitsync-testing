from airflow.models import Variable

from common.custom_hooks.google_sheets_hook import GSheetsHook
import pandas as pd
from common.helpers import process_dynamic_task_sql
from airflow.utils.log.logging_mixin import LoggingMixin

log = LoggingMixin().log


def clean_numbers(x):
    try:
        x = x.replace(" ", "")
        if x[0:1] == '0':
            x = x[1:]
    except Exception as e:
        log.error(e)
    return x


def broadcast_active_nps():

    process_broadcast_active = int(Variable.get("process_broadcast_nps_active",
                                                '0'))
    if process_broadcast_active == 1:
        return

    try:
        sheet_hook = GSheetsHook(
            spreadsheet_id="14aFTBETnr_5o2Uo2I3w1pa8HURfLLgS-d932-nB5390",
            gcp_conn_id="gcp_sheet",
            api_version="v4"
        )
    except Exception as e:
        warning_message = "Google Sheet Hook object could not be instantiated"
        log.warning(warning_message)
        log.error(e, exc_info=True)
        raise e

    try:
        spreadsheet_data = sheet_hook.get_values(
            range_="Form Responses 1!E:E", major_dimension="ROWS").get('values')
    except Exception as e:
        warning_message = "Data retrieval from Google Sheet failed"
        log.warning(warning_message)
        log.error(e, exc_info=True)
        raise e

    if spreadsheet_data is not None:
        try:
            spreadsheet_data = pd.DataFrame(data=spreadsheet_data[1:],
                                            columns=spreadsheet_data[0])
        except Exception as e:
            warning_message = "Pre-processing of spreadsheet data failed"
            log.warning(warning_message)
            log.error(e, exc_info=True)
            raise e

    spreadsheet_data['Your phone number, please :)'] = spreadsheet_data['Your phone number, please :)'].apply(
        clean_numbers)

    phone_numbers = spreadsheet_data['Your phone number, please :)'].tolist()
    phone_numbers_str = ", ".join(phone_numbers)
    query = 'select id from zylaapi.patient_profile where status = 4 AND new_chat=1 AND phoneno not in (' + \
        phone_numbers_str + ')'

    print(phone_numbers)
    print(query)

    message = str(Variable.get("broadcast_active_nps_msg", ''))
    action = "dynamic_message"
    process_dynamic_task_sql(query, message, action)
