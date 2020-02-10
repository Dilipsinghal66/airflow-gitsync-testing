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
                      'code': {'type': 'string', 'required': True},
                      'name': {'type': 'string', 'required': True},
                      'title': {'type': 'string', 'required': True},
                      'phoneno': {'type': 'string', 'required': True},
                      'email': {'type': 'string', 'default': 'None'},
                      'speciality': {'type': 'string', 'default': 'None'},
                      'clinicHospital': {'type': 'string', 'default': 'None'},
                      'location': {'type': 'string', 'default': 'None'},
                      'profile_image': {'type': 'string', 'default': 'None'},
                      'description': {'type': 'string', 'default': 'AZ'},
                      'status': {'type': 'integer', 'default': 4},
                      'Type': {'type': 'integer', 'default': 0},
                      'initiated_by': {'type': 'string',
                                       'default': 'SCHEDULED TASK'},
                      'licenseNumber': {'type': 'string', 'required': True}
                      }}
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
                                         'speciality, clinicHospital, location,'
                                         ' profile_image, description, status, '
                                         'type, initiated_by, licenseNumber',
                           commit_every=100, replace=True)

    except Exception as e:
        log.error(e, exc_info=True)


def initializer():
    """
    Driver function for this script
    :return: Nothing
    """

    config_var = str(Variable.get('doctor_sync_config', '0'))

    if config_var == '0':
        raise ValueError("Config variables unreachable")

    if len(config_var) < 3 or len(config_var) > 3:
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

    sheet_conn = sheet_hook.get_conn()

    if not sheet_conn:
        raise ValueError("Sheet connection is mandatory")

    engine = get_data_from_db(db_type='mysql', conn_id='mysql_monolith')

    if not engine:
        raise ValueError("Couldn't connect to database")

    try:

        spreadsheet_data = sheet_conn.batch_get_values(ranges=range_names,
                                                       major_dimension='ROWS').\
            get('values')

        spreadsheet_data = pd.DataFrame(data=spreadsheet_data[1:],
                                        columns=spreadsheet_data[0])

        dump_data_in_db(table_name=table_name,
                        spreadsheet_data=spreadsheet_data,
                        engine=engine)

    except Exception as e:
        log.err(e, exc_info=True)

