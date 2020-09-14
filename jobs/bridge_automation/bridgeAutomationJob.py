from airflow.models import Variable
from airflow.utils.log.logging_mixin import LoggingMixin
from common.helpers import send_chat_message_patient_id
from datetime import datetime
from common.custom_hooks.google_sheets_hook import GSheetsHook
import pandas as pd
from airflow.models import Variable

def get_patient_data():
    sheetId = "1bEk3es8SPz8VmfQw50XMTdpHkxGkRjQvuAoHV6JMuBU"
    conn_id = "gcp_sheet"
    column_range = "Form Responses 1!A:F"
    major_dimensions = "ROWS"
    
    try:
        sheet_hook = GSheetsHook(
            spreadsheet_id=sheetId,
            gcp_conn_id=conn_id,
            api_version="v4"
        )
    except Exception as e:
        warning_message = "Google Sheet Hook object could not be instantiated"
        log.warning(warning_message)
        log.error(e, exc_info=True)
        raise e
    

    spreadsheet_data = sheet_hook.get_values(range_=column_range, major_dimension=major_dimensions).get('values')
    print(spreadsheet_data)