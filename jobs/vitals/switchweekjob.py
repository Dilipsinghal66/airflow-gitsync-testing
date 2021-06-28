import datetime

from airflow.models import Variable
from airflow.utils.log.logging_mixin import LoggingMixin

from common.db_functions import get_data_from_db


log = LoggingMixin().log


def switch_week_func(week):

    if week == 'A':
        switch = 'B'
    elif week == 'B':
        switch = 'C'
    elif week == 'C':
        switch = 'D'
    elif week == 'D':
        switch = 'A'
    else:
        switch = 'A'

    return switch


def week_switch():
    switch_week_flag = int(
        Variable.get("switch_week_flag", '0'))
    if switch_week_flag == 1:
        return
    try:
        engine = get_data_from_db(db_type="mysql", conn_id="mysql_monolith")
        # print("got db connection from environment")
        connection = engine.get_conn()
        # print("got the connection no looking for cursor")
        cursor = connection.cursor()
        # print("got the cursor")

        cursor.execute("SELECT id, week FROM vitals.week_switches")

        for row in cursor.fetchall():
            switch = switch_week_func(row[1])
            updateweeksqlquery = "UPDATE vitals.week_switches set week = '" + str(switch) + "' where id = " + str(row[0])
            log.info(updateweeksqlquery)
            #cursor.execute(updateweeksqlquery)


        #connection.commit()

    except Exception as e:
        print("Error Exception raised")
        print(e)

