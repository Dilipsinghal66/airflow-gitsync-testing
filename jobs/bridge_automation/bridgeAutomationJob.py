from airflow.models import Variable
import json
from airflow.utils.log.logging_mixin import LoggingMixin
from common.helpers import send_chat_message_patient_id
from datetime import datetime
from common.custom_hooks.google_sheets_hook import GSheetsHook
import pandas as pd

log = LoggingMixin().log

def get_patient_data():

    config_var = Variable.get('bridge_account_creation', None)

    if config_var:
        config_var = json.loads(config_var)
        config_obj = PyJSON(d=config_var)
    else:
        raise ValueError("Config variables not defined")

    sheetId = config_obj.sheet
    conn_id = "gcp_sheet"
    column_range = config_obj.range
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
    log.info(spreadsheet_data)


def initializer(**kargs):
    log.info("Starting....")
    get_patient_data()