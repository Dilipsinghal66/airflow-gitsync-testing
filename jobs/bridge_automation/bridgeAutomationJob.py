from airflow.models import Variable
import json
from airflow.utils.log.logging_mixin import LoggingMixin
from common.helpers import send_chat_message_patient_id
from datetime import datetime
from common.custom_hooks.google_sheets_hook import GSheetsHook
import pandas as pd
from common.pyjson import PyJSON
from common.http_functions import make_http_request

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
    spreadsheet_data = spreadsheet_data[1:]
    for d in spreadsheet_data:
        # Map Gender
        if d[3] == "Male":
            d[3] = 2
        elif d[3] == "Female":
            d[3] = 1
        else:
            d[3] = 0

        # Clean phone number
        d[2] = d[2].replace(" ", "")
        d[2] = d[2].replace("-", "")


    return spreadsheet_data

def create_patient(docCode, phoneno, name, gender):
    #{"phoneno":9599171868,"firstName":"P","lastName":"P","age":18,"location":"na","gender":1,"email":"care@zyla.in","type":1,"status":4}

    payload = {
        "phoneno": phoneno,
        "firstName": name.split(" ")[0],
        "lastName": " ".join(name.split(" ")[1:]),
        "gender": gender,
        "type": 1,
        "email": "care@zyla.in",
        "status": 4
    }
    log.info("Creating patient: ")
    try:
        #status, body = make_http_request(
        #                conn_id="http_patient_url",
        #                endpoint="new", method="POST", payload=payload)
        log.info("Patient created: ")
        log.info(payload)
        # patient_id = body.id
    except Exception as e:
        log.error("Something went wrong ")
        log.error(payload)
        log.error(e)
        
    
    assign_code_payload = {
        "doctorCode": docCode
    }

    #endpoint = str(patient_id) + "/referrer"
    try:
        log.info("Assigning doc code") 
        log.info(assign_code_payload)
        #status, body = make_http_request(
        #                conn_id="http_patient_url",
        #                endpoint=endpoint, method="PUT", payload=payload)
        #)
    except Exception as e:
        log.error(e)



def initializer(**kargs):
    log.info("Starting....")
    patient_data = get_patient_data()
    log.info(patient_data)
    #AZCE1064	9923729053	Niti	Female
    for p in patient_data:
        create_patient(p[0], p[1], p[2], p[3])