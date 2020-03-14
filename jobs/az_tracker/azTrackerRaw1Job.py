from common.db_functions import get_data_from_db
import json
from airflow.models import Variable
from common.custom_hooks.google_sheets_hook import GSheetsHook
from airflow.utils.log.logging_mixin import LoggingMixin
from common.pyjson import PyJSON
import numpy as np


log = LoggingMixin().log


def get_data(table_name, engine, target_fields, query_string):

    sql = "SELECT " + ", ".join(target_fields) + " "

    if table_name:
        sql = sql + " FROM " + table_name

    if query_string:
        sql = sql + query_string

    # sql = "SELECT code, p_tag, patients  from (select * from " \
    #       "zylaapi.doc_profile where code like '%AZ%') x left join " \
    #       "(select x.doc_id, case when y.patient_id is null " \
    #       "then 'non-premium' else 'premium' end as p_tag, " \
    #       "count(distinct x.patient_id) as patients " \
    #       "from (select referred_by as doc_id, id as patient_id from " \
    #       "zylaapi.patient_profile group by 1,2) x left join (select " \
    #       "patient_id from zylaapi.prescription_verification " \
    #       "where status =1 group by 1) y on " \
    #       "x.patient_id = y.patient_id group by 1,2) y on x.id = y.doc_id;"

    log.debug(sql)

    try:

        data_df = engine.get_pandas_df(sql=sql)
        log.debug(data_df.head())

    except Exception as e:
        warning_message = "Could not get data from Database"
        log.warning(warning_message)
        log.error(e, exc_info=True)
        raise e

    return data_df


def update_spreadsheet(sheet_hook, data, sheet):

    data.replace(np.nan, '', inplace=True)

    # values = list(data.columns)
    values = data.values.tolist()

    response = sheet_hook.append_values(range_=sheet.column_range,
                                        values=values,
                                        major_dimension=sheet.major_dimensions,
                                        insert_data_option=
                                        sheet.insert_data_option,
                                        include_values_in_response=True,
                                        )

    return response


def initializer(**kwargs):
    """
    Driver function for this script
    """

    config_var = Variable.get('az_tracker_raw1_config', None)

    if config_var:
        config_var = json.loads(config_var)
        config_obj = PyJSON(d=config_var)
    else:
        raise ValueError("Config variables not defined")

    try:
        gcp = config_obj.gcp
        sheet = config_obj.sheet
        db = config_obj.db
        query = config_obj.query

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
                           target_fields=query.fields,
                           query_string=query.query_string
                           )

    except Exception as e:
        warning_message = "Data retrieval from DB unsuccessful"
        log.warning(warning_message)
        log.error(e, exc_info=True)
        raise e

    log.debug("Data successfully retrieved from database")

    response = update_spreadsheet(sheet_hook=sheet_hook,
                                  data=data_df,
                                  sheet=sheet)

    log.info(response)
