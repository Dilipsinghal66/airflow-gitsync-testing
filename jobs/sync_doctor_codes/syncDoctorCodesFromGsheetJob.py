from common.db_functions import get_data_from_db
import json
import pandas as pd
from airflow.models import Variable
from common.custom_hooks.google_sheets_hook import GSheetsHook
from cerberus import Validator
from airflow.utils.log.logging_mixin import LoggingMixin
from common.pyjson import PyJSON

log = LoggingMixin().log


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
        log.warning(validator_obj.errors)

    return validation_result


def dump_data_in_db(table_name, spreadsheet_data, engine, schema,
                    target_fields, defaults, unique_fields):
    """
    Dumps data into the database
    :param unique_fields: Unique keys
    :param defaults: Default values
    :param target_fields: Fields to be updated in database
    :param schema: Validation schema
    :param table_name: Name of the table where data is to be written
    :param spreadsheet_data: Data from the GSheetsHook
    :param engine: MySqlHook object from common.db_functions
    :return:
    """
    spreadsheet_data['description'] = defaults.description
    spreadsheet_data['status'] = defaults.status
    spreadsheet_data['type'] = defaults.type
    spreadsheet_data['initiated_by'] = defaults.initiated_by
    spreadsheet_data['licenseNumber'] = spreadsheet_data[
                                        defaults.license_number]

    row_list = []

    schema = schema.to_dict()
    validator_obj = Validator(schema)

    spreadsheet_list = spreadsheet_data.values.tolist()

    for row in range(len(spreadsheet_list)):

        if schema_validation(validator_obj=validator_obj,
                             spreadsheet_row=spreadsheet_list[row]):

            log.debug("Validation successful for record " + str(row))

            if len(spreadsheet_list[row]) == len(target_fields):
                row_list.append(spreadsheet_list[row])

        else:
            warning_message = "Validation failed for record " + str(row)
            log.warning(warning_message)

    try:
        log.debug("Fields being replaced are as follows: ")
        log.debug(target_fields)
        log.debug("Number of fields: " + str(len(target_fields)))
        log.debug("Number of records: " + str(len(row_list)))

        if row_list:
            log.debug("Number of fields in a record: " + str(len(row_list[0])))
            engine.upsert_rows(table=table_name,
                               rows=row_list,
                               target_fields=target_fields,
                               commit_every=1,
                               unique_fields=unique_fields
                               )
            log.info("Data successfully updated in mysql database")

        else:
            warning_message = "No data updated in mysql database"
            log.warning(warning_message)

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

    if config_var:
        config_var = json.loads(config_var)
        config_obj = PyJSON(d=config_var)
    else:
        raise ValueError("Config variables not defined")

    try:
        gcp = config_obj.gcp
        sheet = config_obj.sheet
        db = config_obj.db
        validation_schema = config_obj.validation
        defaults = config_obj.defaults

    except Exception as e:
        warning_message = "Couldn't get config variables"
        log.warning(warning_message)
        log.error(e, exc_info=True)
        raise e

    try:
        sheet_hook = GSheetsHook(
            spreadsheet_id=sheet.id,
            gcp_conn_id=gcp.conn_id,
            api_version="v4"
        )
    except Exception as e:
        warning_message = "Google Sheet Hook object could not be instantiated"
        log.warning(warning_message)
        log.error(e, exc_info=True)
        raise e

    try:
        engine = get_data_from_db(db_type=db.type, conn_id=db.conn_id)

    except Exception as e:
        warning_message = "Connection to mysql database failed."
        log.warning(warning_message)
        log.error(e, exc_info=True)
        raise e

    try:

        spreadsheet_data = sheet_hook.get_values(range_=sheet.column_range,
                                                 major_dimension=sheet.
                                                 major_dimensions).\
                                                 get('values')

    except Exception as e:
        warning_message = "Data retrieval from Google Sheet failed"
        log.warning(warning_message)
        log.error(e, exc_info=True)
        raise e

    if spreadsheet_data is not None:

        try:
            spreadsheet_data = pd.DataFrame(data=spreadsheet_data[1:],
                                            columns=spreadsheet_data[0])
            spreadsheet_data.drop(columns=sheet.drop_columns,
                                  axis=1,
                                  inplace=True)
        except Exception as e:
            warning_message = "Pre-processing of spreadsheet data failed"
            log.warning(warning_message)
            log.error(e, exc_info=True)
            raise e

        try:
            dump_data_in_db(table_name=db.table_name,
                            spreadsheet_data=spreadsheet_data,
                            engine=engine,
                            schema=validation_schema.schema,
                            target_fields=db.fields,
                            defaults=defaults,
                            unique_fields=db.unique_fields)

            log.info("Script executed successfully")

        except Exception as e:
            warning_message = "Data dumping into database failed"
            log.warning(warning_message)
            log.error(e, exc_info=True)
            raise e

    else:
        warning_message = "No data received from Google Sheets API"
        log.warning(warning_message)
