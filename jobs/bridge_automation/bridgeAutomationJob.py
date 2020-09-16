from airflow.models import Variable
import json
from airflow.utils.log.logging_mixin import LoggingMixin
from common.helpers import send_chat_message_patient_id
from datetime import datetime
from common.custom_hooks.google_sheets_hook import GSheetsHook
import pandas as pd
from common.pyjson import PyJSON
from common.http_functions import make_http_request
from airflow.hooks.http_hook import HttpHook

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
    spreadsheet_data = pd.DataFrame(data=spreadsheet_data[1:],
                                columns=spreadsheet_data[0])
    spreadsheet_data.fillna('', inplace=True)

    log.info(spreadsheet_data)

    try:
        for i, row in spreadsheet_data.iterrows():
            #log.info(d)
            # Map Gender
            if row["Patient's gender"] == "Male":
                row["Patient's gender"] = 2
            elif row["Patient's gender"] == "Female":
                row["Patient's gender"] = 1
            else:
                row["Patient's gender"] = 0

            # Clean phone number
            row["Patient's phone number"] = row["Patient's phone number"].replace(" ", "")
            row["Patient's phone number"] = row["Patient's phone number"].replace("-", "")
            log.info(row)
    except Exception as e:
        log.error(e)


    return spreadsheet_data

def create_patient(docCode, phoneno, name, gender):
    #{"phoneno":9599171868,"firstName":"P","lastName":"P","age":18,"location":"na","gender":1,"email":"care@zyla.in","type":1,"status":4}

    config_var = Variable.get('bridge_account_creation', None)

    if config_var:
        config_var = json.loads(config_var)
        config_obj = PyJSON(d=config_var)
    else:
        raise ValueError("Config variables not defined")
    
    headers = {
        "auth_token": config_obj['auth_token'],
        "phone_no": config_obj['phone_no'],
        "client": config_obj['client'],
        "Content-Type": "application/json"
    }

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
        hook_obj = HttpHook(method="POST", http_conn_id="http_patient_url")
        payload = json.dumps(payload)
        response = hook_obj.run(endpoint="new", data=payload, headers=headers)
        body = response.json() 
        status = response.status_code
        log.info("Patient created: ")
        log.info(payload)
        log.info(body)
        patient_id = body['id']
    except Exception as e:
        log.error("Something went wrong ")
        log.error(payload)
        log.error(e)
        
    
    assign_code_payload = {
        "doctorCode": docCode
    }
    
    if patient_id:
        endpoint = str(patient_id) + "/referrer"
        try:
            log.info("Assigning doc code") 
            log.info(assign_code_payload)
            hook_obj = HttpHook(method="PUT", http_conn_id="http_patient_url")
            assign_code_payload = json.dumps(assign_code_payload)
            response = hook_obj.run(endpoint=endpoint, data=payload, headers=headers)
            body = response.json() 
            status = response.status_code
        except Exception as e:
            log.error(e)



def initializer(**kargs):
    log.info("Starting....")
    patient_data = get_patient_data()
    log.info(patient_data)
    log.info("Creating a test patient")
    create_patient("ZH0000", "9999999999", "Test", 2)
    #AZCE1064	9923729053	Niti	Female
    #for i, row in patient_data.iterrows():
    #    create_patient(row["Doctor code/ Phone number"], row["Patient's phone number"], row["Patient's name"], row["Patient's gender"])