from common.db_functions import get_data_from_db
import json
from airflow.models import Variable
from common.custom_hooks.google_sheets_hook import GSheetsHook
from airflow.utils.log.logging_mixin import LoggingMixin
from common.pyjson import PyJSON
import numpy as np

log = LoggingMixin().log


def fixing_names(df):

    df['spoc_name'] = df['spoc_name'].apply(lambda x: " ".join(map(lambda y:
                                            y[:1].upper() + y[1:].lower(),
                                                                   x.split())))

    df['tbm_name'] = df['tbm_name'].apply(lambda x: " ".join(map(lambda y:
                                          y[:1].upper() + y[1:].lower(),
                                                                 x.split())))

    return df


def get_data_multiple_queries(table_name, engine, sheet):

    """
    :param table_name: The table which will be queried
    :param engine: SQL connection object
    :param sheet: A set of config for sheet
    :return: pandas dataframe
    """

    data_df_query1 = get_data(table_name=table_name, engine=engine,
                              target_fields=sheet.query.fields[0],
                              query_string=sheet.query.query_string[0])

    data_df_query1 = fixing_names(data_df_query1)

    log.info("Query 1 successful")
    log.debug(data_df_query1.head())

    for i in range(1, len(sheet.query.query_string)):

        data_df = get_data(table_name=table_name, engine=engine,
                           target_fields=sheet.query.fields[i],
                           query_string=sheet.query.query_string[i])

        data_df = fixing_names(data_df)
        log.debug(data_df.head())
        log.info("Query " + str(i + 1) + " successful")

        data_df_query1 = data_df_query1.merge(data_df, on=sheet.merge_key,
                                              how='outer')
        log.debug(data_df_query1.head())

    log.info("Merge Successful")

    data_df = data_df_query1[sheet.query.column_order]

    data_df.rename(columns=sheet.query.column_names.to_dict(),
                   inplace=True)

    return data_df


def get_data(table_name, engine, target_fields, query_string):

    """
    :param table_name: table in which query should be executed
    :param engine: sql connection object
    :param target_fields: fields required in output
    :param query_string: query
    :return: Dataframe
    """

    sql = "SELECT " + ", ".join(target_fields) + " "

    if table_name:
        sql = sql + " FROM " + table_name

    if query_string:
        sql = sql + query_string

    log.debug(sql)

    try:

        data_df = engine.get_pandas_df(sql=sql)
        log.info("Query executed successfully")
        log.debug(data_df.head())

    except Exception as e:
        warning_message = "Query Unsuccessful"
        log.warning(warning_message)
        log.error(e, exc_info=True)
        raise e

    return data_df


def update_spreadsheet(sheet_hook, data, sheet):

    """
    :param sheet_hook: Google sheet hook
    :param data: Dataframe
    :param sheet: Config var for sheet configurations
    :return: dict
    """

    data.replace(np.nan, '', inplace=True)

    if not data.empty:

        log.info("Clearing the spreadsheet")

        sheet_hook.clear(range_=sheet.column_range)

        values = data.values.tolist()
        values.insert(0, data.columns.values.tolist())

        for i in range(len(values)):
            log.debug(values[i])

        try:

            log.info("Overwriting the spreadsheet data")
            response = sheet_hook.update_values(
                range_=sheet.column_range,
                values=values,
                major_dimension=sheet.major_dimensions,
                include_values_in_response=True
                )
            log.info(response)
            log.info("Data overwritten successfully")

        except Exception as e:
            warning_message = "Data overwriting unsuccessful"
            log.warning(warning_message)
            log.error(e, exc_info=True)
            raise e

    else:
        log.warning("No data received from the query")


def initializer(**kwargs):
    """
    Driver function for this script
    """

    config_var = Variable.get('az_tracker_config', None)

    if config_var:
        config_var = json.loads(config_var)
        config_obj = PyJSON(d=config_var)
    else:
        raise ValueError("Config variables not defined")

    try:
        gcp = config_obj.gcp
        sheet = config_obj.sheet
        raw1 = sheet.raw1
        raw2 = sheet.raw2
        db = config_obj.db

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
        data_df = get_data(table_name=db.table_name,
                           engine=engine,
                           target_fields=raw2.query.fields,
                           query_string=raw2.query.query_string
                           )

        update_spreadsheet(sheet_hook=sheet_hook,
                           data=data_df,
                           sheet=raw2)

        log.info("Task on Raw 2 spreadsheet successful")

    except Exception as e:
        warning_message = "Task on Raw 2 spreadsheet unsuccessful"
        log.warning(warning_message)
        log.error(e, exc_info=True)
        raise e

    try:
        data_df_merged = get_data_multiple_queries(table_name=db.table_name,
                                                   engine=engine,
                                                   sheet=raw1
                                                   )

        update_spreadsheet(sheet_hook=sheet_hook,
                           data=data_df_merged,
                           sheet=raw1)

        log.info("Task on Raw 1 spreadsheet successful")

    except Exception as e:
        warning_message = "Task on Raw 1 spreadsheet unsuccessful"
        log.warning(warning_message)
        log.error(e, exc_info=True)
        raise e
