from airflow.models import Variable
from airflow.utils.log.logging_mixin import LoggingMixin
from common.helpers import process_custom_message_sql
from common.db_functions import get_data_from_db
from datetime import datetime
import json

log = LoggingMixin().log

def broadcast_pfizer_pregnancy():
    broadcast_pfizer_pregnancy_flag = int(Variable.get("broadcast_pfizer_pregnancy", '0'))
    if broadcast_pfizer_pregnancy_flag == 1:
        return

    # Corrected SQL query with proper parentheses
    sql_query = (
        "SELECT id, "
        "concat('W', ROUND(((datediff(curdate(), delivery_date - INTERVAL '273' DAY) / 7) + 1), 0)) AS PREGNANCY_WEEK "
        "FROM zylaapi.patients_base_view AS pp "
        "LEFT JOIN (SELECT id, phoneno FROM zylaapi.auth) AS au ON au.phoneno = pp.phoneno "
        "LEFT JOIN (SELECT user_id, delivery_date FROM assessment.delivery_dates) AS DD ON DD.user_id = au.id "
        "WHERE client_code IN ('NA', 'ND')"
    )

    # Fetching data from the database
    sql_data = get_data_from_db(db_type="mysql", conn_id="mysql_monolith", sql_query=sql_query, execute_query=True)

    # Creating a map of user IDs
    user_id_map = {}
    if sql_data:
        for record in sql_data:
            key = record[1]  # Assuming this is the unique ID
            value = record[0]  # Assuming this is the pregnancy week
            user_id_map[key] = value

    log.info(user_id_map)

    # Processing the message
    message = json.loads(Variable.get("broadcast_pfizer_pregnancy_msg", '{}'))
    date_string = f'{datetime.now():%Y-%m-%d %H:%M:%S%z}'
    group_id = "broadcast_pfizer_pregnancy " + date_string

    log.info(f"SQL Query: {sql_query}\nMessage: {message}")
    
    
    for n in range(1, 41):
        week_key = f'W{n}'
        if week_key in message:
            message1 = message[week_key]
            log.info(f"Message_in_for: {message1}")
            sql_query_1 = (
        "SELECT id, "
        "concat('W', ROUND(((datediff(curdate(), delivery_date - INTERVAL '273' DAY) / 7) + 1), 0)) AS PREGNANCY_WEEK "
        "FROM zylaapi.patients_base_view AS pp "
        "LEFT JOIN (SELECT id, phoneno FROM zylaapi.auth) AS au ON au.phoneno = pp.phoneno "
        "LEFT JOIN (SELECT user_id, delivery_date FROM assessment.delivery_dates) AS DD ON DD.user_id = au.id  "
        f"WHERE client_code IN ('NA', 'ND') And firstName not like '%Test%' and ROUND(((datediff(curdate(), delivery_date - INTERVAL '273' DAY) / 7) + 1), 0) = {n}"
    )
            process_custom_message_sql(sql_query_1, message1, group_id)
        else:
            log.info(f"No user ID found for week {week_key}")