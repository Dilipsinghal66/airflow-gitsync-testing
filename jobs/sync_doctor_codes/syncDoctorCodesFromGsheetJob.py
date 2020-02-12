from common.db_functions import get_data_from_db
import pandas as pd
from airflow.models import Variable
from common.custom_hooks.google_sheets_hook import GSheetsHook
from cerberus import Validator
from airflow.utils.log.logging_mixin import LoggingMixin


log = LoggingMixin().log


def make_schema():
    """
    Making validation schema
    :return: Validation schema
    """
    log.info("Making validation schema")

    schema = {'row':
              {'type': 'list',
               'items':
               [{'type': 'string', 'required': True},
                {'type': 'string', 'required': True},
                {'type': 'string', 'required': True},
                {'type': 'string', 'required': True,
                    'minlength': 6, 'maxlength': 10},
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


def schema_validation(validator_obj, spreadsheet_row):
    """
    Validation of record to be inserted in database
    :param validator_obj: Validator object
    :param spreadsheet_row: A record from the spreadsheet
    :return: bool
    """
    log.debug("Record: " + str(spreadsheet_row))
    record = {'row': spreadsheet_row}

    validation_result = validator_obj.validate(record)

    if not validation_result:
        log.debug(validator_obj.errors)

    return validation_result


def dump_data_in_db(table_name, spreadsheet_data, engine):
    """
    Dumps data into the database
    :param table_name: Name of the table where data is to be written
    :param spreadsheet_data: Data from the GSheetsHook
    :param engine: MySqlHook object from common.db_functions
    :return:
    """
    spreadsheet_data['description'] = 'AZ'
    spreadsheet_data['status'] = 4
    spreadsheet_data['type'] = 0
    spreadsheet_data['initiated_by'] = 'SCHEDULED TASK'
    spreadsheet_data['licenseNumber'] = spreadsheet_data['Doctor Code']

    row_list = []

    schema = make_schema()
    log.info("Validation schema initialised")

    validator_obj = Validator(schema)

    target_fields = ['code', 'name', 'title', 'phoneno', 'email', 'speciality',
                     'clinicHospital', 'location', 'profile_image',
                     'description', 'status', 'type', 'initiated_by',
                     'licenseNumber']

    spreadsheet_list = spreadsheet_data.values.tolist()

    for row in range(len(spreadsheet_list)):

        if schema_validation(validator_obj=validator_obj,
                             spreadsheet_row=spreadsheet_list[row]):

            log.debug("Validation successful for record " + str(row))

            if len(spreadsheet_list[row]) == len(target_fields):
                row_list.append(spreadsheet_list[row])

        else:
            log.debug("Validation failed for record " + str(row))

    try:
        log.info("Fields being replaced are as follows: ")
        log.info(target_fields)
        log.info("Number of fields: " + str(len(target_fields)))
        log.info("Number of fields in a record: " + str(len(row_list[0])))
        log.info("Number of records: " + str(len(row_list)))

        for i in range(len(row_list)):
            log.debug(row_list[i])

        if len(row_list) > 0:
            engine.insert_rows(table=table_name,
                               rows=row_list,
                               target_fields=target_fields,
                               commit_every=100,
                               replace=True
                               )

            log.info("Data successfully updated in mysql database")

        else:
            log.info("No data updated in mysql database")

    except Exception as e:
        warning_message = "Failed to update data in mysql database"
        log.warning(warning_message)
        log.error(e, exc_info=True)
        raise e


def initializer():
    """
    Driver function for this script
    :return:
    """
    config_var = Variable.get('doctor_sync_config', None)

    if not config_var:
        raise ValueError("Config variables not defined")

    config_var = config_var.split('|')

    if len(config_var) is not 3:
        raise ValueError("Incomplete config variables")

    spreadsheet_id = config_var[0]
    gcp_conn_id = config_var[1]
    table_name = config_var[2]

    try:
        sheet_hook = GSheetsHook(
            spreadsheet_id=spreadsheet_id,
            gcp_conn_id=gcp_conn_id,
            api_version="v4"
        )

    except Exception as e:
        warning_message = "Google Sheet Hook object could not be instantiated"
        log.warning(warning_message)
        log.error(e, exc_info=True)
        raise e

    try:
        engine = get_data_from_db(db_type='mysql', conn_id='mysql_monolith')

    except Exception as e:
        warning_message = "Connection to mysql database failed."
        log.warning(warning_message)
        log.error(e, exc_info=True)
        raise e

    try:

        spreadsheet_data = sheet_hook.get_values(range_='Sheet1!A:J',
                                                 major_dimension='ROWS'
                                                 ).get('values')

    except Exception as e:
        warning_message = "Data retrieval from Google Sheet failed"
        log.warning(warning_message)
        log.error(e, exc_info=True)
        raise e

    if spreadsheet_data is not None:
        spreadsheet_data = pd.DataFrame(data=spreadsheet_data[1:],
                                        columns=spreadsheet_data[0])
        spreadsheet_data.drop(columns=['Whatsapp No. (+91)'],
                              axis=1,
                              inplace=True)

        try:
            dump_data_in_db(table_name=table_name,
                            spreadsheet_data=spreadsheet_data,
                            engine=engine)

            log.info("Script executed successfully")

        except Exception as e:
            warning_message = "Data dumping into database failed"
            log.warning(warning_message)
            log.error(e, exc_info=True)
            raise e

    else:
        warning_message = "No data received from Google Sheets API"
        log.warning(warning_message)
