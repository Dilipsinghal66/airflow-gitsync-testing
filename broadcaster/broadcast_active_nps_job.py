from airflow.models import Variable

from common.custom_hooks.google_sheets_hook import GSheetsHook
import pandas as pd
from common.helpers import process_dynamic_task_sql
from airflow.utils.log.logging_mixin import LoggingMixin

log = LoggingMixin().log


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

    print(spreadsheet_data)

    # sql_query = str(Variable.get("broadcast_active_nps_sql_query",
    #                              '''

    #                             '''))

    # message = str(Variable.get("broadcast_active_nps_msg", ''))
    # action = "dynamic_message"
    # process_dynamic_task_sql(sql_query, message, action)
