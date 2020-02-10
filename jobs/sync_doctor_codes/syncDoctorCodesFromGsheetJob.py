from common.db_functions import get_data_from_db
import pandas as pd
from airflow.models import Variable
from common.custom_hooks.google_sheets_hook import GSheetsHook
from cerberus import Validator
from airflow.utils.log.logging_mixin import LoggingMixin

log = LoggingMixin().log


def schema_validation(schema, spreadsheet_row):
    """
    Validation of record to be inserted in database
    :param schema: Validation Schema
    :param spreadsheet_row: A record from the spreadsheet
    :return: bool
    """
    record = {'row': spreadsheet_row}

    v = Validator(schema)
    return v.validate(record, schema)


def make_schema():
    """
    Making validation schema
    :return: Validation schema
    """
    log.info("Making validation schema")

    schema = {'row': {'type': 'list',
              'items':
                      [{'type': 'string', 'required': True},
                       {'type': 'string', 'required': True},
                       {'type': 'string', 'required': True},
                       {'type': 'string', 'required': True},
                       {'type': 'string', 'default': 'None'},
                       {'type': 'string', 'default': 'None'},
                       {'type': 'string', 'default': 'None'},
                       {'type': 'string', 'default': 'None'},
                       {'type': 'string', 'default': 'None'},
                       {'type': 'string', 'default': 'AZ'},
                       {'type': 'integer', 'default': 4},
                       {'type': 'integer', 'default': 0},
                       {'type': 'string', 'default': 'SCHEDULED TASK'},
                       {'type': 'string', 'required': True}]}}

    return schema


def dump_data_in_db(table_name, spreadsheet_data, engine):
    """
    Dumps data into the database
    :param table_name: Name of the table where data is to be written
    :param spreadsheet_data: Data from the GSheetsHook
    :param engine: MySqlHook object from common.db_functions
    :return: Nothing
    """

    spreadsheet_data['description'] = 'AZ'
    spreadsheet_data['status'] = 4
    spreadsheet_data['type'] = 0
    spreadsheet_data['initiated_by'] = 'SCHEDULED TASK'
    spreadsheet_data['licenseNumber'] = spreadsheet_data['Doctor Code']

    row_list = [[]]

    schema = make_schema()
    log.info("Validation schema received")

    for row in range(len(spreadsheet_data)):

        if schema_validation(schema, spreadsheet_data[row]):
            log.info("Validation successful for record " + str(row))
            row_list.append(spreadsheet_data[row])

        else:
            log.error("Validation failed for record " + str(row))

    try:
        engine.insert_rows(table_name, row_list,
                           target_fields='code, name, title, phoneno, email, '
                                         'speciality, clinicHospital, '
                                         'location, profile_image, '
                                         'description, status, type, '
                                         'initiated_by, licenseNumber',
                           commit_every=100, replace=True)

    except Exception as e:
        warning_message = "Data insertion into mysql database failed"
        log.warning(warning_message)
        log.err(e, exc_info=True)
        raise e


def initializer():
    """
    Driver function for this script
    :return: Nothing
    """

    config_var = Variable.get('doctor_sync_config', None)

    if not config_var:
        raise ValueError("Config variables not defined")

    if len(config_var) != 3:
        raise ValueError("Incomplete config variables")

    config_var.split('|')
    spreadsheet_id = config_var[0]
    gcp_conn_id = config_var[1]
    table_name = config_var[2]

    range_names = ['Sheet1!A:D', 'Sheet1!F:J']

    sheet_hook = GSheetsHook(
        spreadsheet_id=spreadsheet_id,
        gcp_conn_id=gcp_conn_id,
        api_version="v4"
    )

    try:
        sheet_conn = sheet_hook.get_conn()

    except Exception as e:
        warning_message = "Google Sheets API failed"
        log.warning(warning_message)
        log.err(e, exc_info=True)
        raise e

    try:
        engine = get_data_from_db(db_type='mysql', conn_id='mysql_monolith')

    except Exception as e:
        warning_message = "Connection to mysql database failed."
        log.warning(warning_message)
        log.err(e, exc_info=True)
        raise e

    try:
        spreadsheet_data = sheet_conn.batch_get_values(ranges=range_names,
                                                       major_dimension='ROWS')\
            .get('values')

    except Exception as e:
        warning_message = "Data retrieval from Google Sheet failed"
        log.warning(warning_message)
        log.err(e, exc_info=True)
        raise e

    spreadsheet_data = pd.DataFrame(data=spreadsheet_data[1:],
                                    columns=spreadsheet_data[0])

    try:
        dump_data_in_db(table_name=table_name,
                        spreadsheet_data=spreadsheet_data,
                        engine=engine)

    except Exception as e:
        warning_message = "Data dumping into database failed"
        log.warning(warning_message)
        log.err(e, exc_info=True)
        raise e
