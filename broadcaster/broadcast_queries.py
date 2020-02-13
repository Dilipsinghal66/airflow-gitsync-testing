# import json
from airflow.models import Variable
from airflow.utils.log.logging_mixin import LoggingMixin
# from common.pyjson import PyJSON
# import datetime
from common.db_functions import get_data_from_db
log = LoggingMixin().log

"""
All paid Male patients on bridge (status = 4)
All paid female patients on bridge (status = 4)
All paid patients on bridge who shared reporting between 6 pm yesterday 
to 6 pm today
All paid patients on bridge who have never listened to meditations until today
"""


def broadcast_queries():
    """

    :return:
    """

    sql_query_male = str(Variable.get("paid_male_patients",
                                      'SELECT id FROM '
                                      'zylaapi.patient_profile '
                                      'WHERE status = 4 AND gender = 1'))

    sql_query_female = str(Variable.get("paid_female_patients",
                                        'SELECT id FROM '
                                        'zylaapi.patient_profile '
                                        'WHERE status = 4 AND gender = 2'))

    sql_query_meditation = str(Variable.get("no_meditation",
                                            'SELECT id FROM '
                                            'zylaapi.patient_profile '
                                            'WHERE id NOT IN '
                                            '(SELECT DISTINCT '
                                            'patientId FROM '
                                            'zylaapi.meditationLogs)'))

    # config_var = Variable.get('broadcast_query_config', None)
    #
    # if config_var:
    #     config_var = json.loads(config_var)
    #     config_obj = PyJSON(d=config_var)
    # else:
    #     raise ValueError("Config variables not defined")
    #
    # try:
    #     db = config_obj.db
    #
    # except Exception as e:
    #     warning_message = "Couldn't get config variables"
    #     log.warning(warning_message)
    #     log.error(e, exc_info=True)
    #     raise e

    engine = get_data_from_db(db_type='mysql', conn_id='mysql_monolith')
    # timeSlot = datetime.datetime.now()

    # sql = ["SELECT id FROM zylaapi.patient_profile "
    #        "WHERE status = 4 AND gender = 1",
    #        "SELECT id FROM zylaapi.patient_profile "
    #        "WHERE status = 4 AND gender = 2",
    #        "SELECT id FROM zylaapi.patient_profile "
    #        "WHERE id NOT IN (SELECT DISTINCT patientId "
    #        "FROM zylaapi.meditationLogs)",
    #        ]
    #
    try:

        data1 = engine.get_records(sql=sql_query_male, parameters='id')
        log.debug(sql_query_male)
        log.debug(data1)

        data2 = engine.get_records(sql=sql_query_female, parameters='id')
        log.debug(sql_query_male)
        log.debug(data2)

        data3 = engine.get_records(sql=sql_query_meditation, parameters='id')
        log.debug(sql_query_male)
        log.debug(data3)

    except Exception as e:
        warning_message = "Query unsuccessful"
        log.warning(warning_message)
        log.error(e, exc_info=True)
        raise e



    # projection = {
    #     "_id": 1
    # }
    # _filter = {
    #     "lastReported": {"$gte": ""}, {}
    # }
    #
    # mongo_data = get_data_from_db(db_type='mongo', conn_id="mongo_user_db",
    #                               collection="user_activity",
    #                               filter=_filter,
    #                               projection=projection)