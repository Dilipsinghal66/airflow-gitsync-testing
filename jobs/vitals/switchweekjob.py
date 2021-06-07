import datetime

from airflow.models import Variable
from airflow.utils.log.logging_mixin import LoggingMixin

from common.db_functions import get_data_from_db

PAGE_SIZE = 1000

log = LoggingMixin().log

defVitalGroups = Variable.get("default_vital_groups", deserialize_json=True)

def switch_week_func(id,week):
    try:
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
            # engine = create_engine('mysql+pymysql://user:user@123@localhost/zylaapi')  # noqa E303
            # print("starting create vitals job")
            engine = get_data_from_db(db_type="mysql",
                                      conn_id="vital_db")
            # print("got db connection from environment")
            connection = engine.get_conn()
            # print("got the connection no looking for cursor")
            cursor = connection.cursor()
            # print("got the cursor")
            updateweeksqlquery = "UPDATE vital.week_switches set week= '" + switch + "' where id="+ id
            cursor.execute(updateweeksqlquery)
            connection.commit()
    except Exception as e:
        log.error(e)
        raise e

def week_switch():
    process_broadcast_remind_webinar = int(
        Variable.get("process_broadcast_remind_webinar", '0'))
    if process_broadcast_remind_webinar == 1:
        return
    try:
        engine = get_data_from_db(db_type="mysql", conn_id="vital_db")
        # print("got db connection from environment")
        connection = engine.get_conn()
        # print("got the connection no looking for cursor")
        cursor = connection.cursor()
        # print("got the cursor")

        cursor.execute("SELECT id,week FROM vital.week_switches")

        for row in cursor.fetchall():
            switch_week_func(row[0],row[1])

    except Exception as e:
        print("Error Exception raised")
        print(e)

