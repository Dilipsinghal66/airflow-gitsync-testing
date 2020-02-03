from common.db_functions import get_data_from_db
import pandas as pd
from airflow.models import Variable
from common.custom_hooks.google_sheets_hook import GSheetsHook
from cerberus import Validator


def schema_validation(spreadsheet_row):
    """
    Validation of record to be inserted in database
    :param spreadsheet_row: A record from the spreadsheet
    :return: bool
    """

    schema = {'rows': {'type': 'list',
                       'code': {'type': 'string', 'required': True},
                       'name': {'type': 'string', 'required': True},
                       'title': {'type': 'string', 'required': True},
                       'phoneno': {'type': 'string', 'required': True},
                       'email': {'type': 'string', 'default_setter': "None"},
                       'speciality': {'type': 'string', 'default_setter': "None"
                                      },
                       'clinicHospital': {'type': 'string', 'default_setter':
                                                            "None"},
                       'location': {'type': 'string', 'default_setter': "None"},
                       'profile_image': {'type': 'string', 'default_setter':
                                                           "None"},
                       'description': {'type': 'string', 'default_setter':
                                                         "AZ"},
                       'status': {'type': 'string', 'default_setter': '4'},
                       'Type': {'type': 'integer', 'default_setter': '0'},
                       'initiated_by': {'type': 'string',
                                        'default_setter': "SCHEDULED TASK"},
                       'licenseNumber': {'type': 'string', 'required': True},
                       }}
    document = {'rows': spreadsheet_row}
    v = Validator(schema)
    return v.validate(document, schema)


def dump_data_in_db(table_name, spreadsheet_data, engine):
    """
    Dumps data into the database
    :param table_name: Name of the table where data is to be written
    :param spreadsheet_data: Data from the GSheetsHook
    :param engine: MySqlHook object from common.db_functions
    :return: Nothing
    """

    spreadsheet_data['description'] = "AZ"
    spreadsheet_data['status'] = 4
    spreadsheet_data['type'] = 0
    spreadsheet_data['initiated_by'] = "SCHEDULED TASK"
    spreadsheet_data['licenseNumber'] = spreadsheet_data['Doctor Code']

    row_list = [[]]

    for row in range(len(spreadsheet_data)):

        if schema_validation(spreadsheet_data[row]):
            row_list.append(spreadsheet_data[row])

    engine.insert_rows(table_name, row_list,
                       target_fields='code, name, title, phoneno, email, '
                                     'speciality, clinicHospital, location, '
                                     'profile_image, description, status, '
                                     'type, initiated_by, licenseNumber',
                       commit_every=100, replace=True)


def initializer():
    """
    Driver function for this script
    :return: Nothing
    """

    config_var = str(Variable.get('config_var', '0'))

    if config_var == '0':
        return

    if len(config_var) < 3 or len(config_var) > 3:
        return

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

    spreadsheet_data = sheet_conn.batch_get_values(ranges=range_names,
                                                   major_dimension='ROWS').\
        get('values')

    spreadsheet_data = pd.DataFrame(spreadsheet_data[1:],
                                    columns=spreadsheet_data[0])

    engine = get_data_from_db(db_type="mysql", conn_id="mysql_monolith")

    dump_data_in_db(table_name, spreadsheet_data, engine)
