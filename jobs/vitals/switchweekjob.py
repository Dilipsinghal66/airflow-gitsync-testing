import datetime

from airflow.models import Variable
from airflow.utils.log.logging_mixin import LoggingMixin

from common.db_functions import get_data_from_db

PAGE_SIZE = 1000

log = LoggingMixin().log

defVitalGroups = Variable.get("default_vital_groups", deserialize_json=True)

def switch_week_func(**kwargs):
    try:
        disable_vital_create = int(Variable.get("disable_vital_create", '0'))
        if not disable_vital_create:
            date = datetime.datetime.today()
            timedelta = datetime.timedelta(hours=5, minutes=30)
            todayDate = date + timedelta

            vital_switch_flag = str(Variable.get("vital_switch_flag", ''))

            if not vital_switch_flag:
                vital_switch_flag = 'X,' + str(todayDate)
                log.info("Didn't get return value so today's date")

            log.info("vital_switch_flag = " + vital_switch_flag)
            switchArr = vital_switch_flag.split(",")
            switch = switchArr[0]
            dateTimeStr = switchArr[1]
            dateTimeObj = datetime.datetime.strptime(dateTimeStr,
                                                     '%Y-%m-%d %H:%M:%S.%f')

            weekday = todayDate.weekday()
            switchDaysDiff = (todayDate - dateTimeObj).days

            log.info("switch days diff " + str(switchDaysDiff))
            if weekday == 5 and switchDaysDiff >= 4:
                if switch == 'A':
                    switch = 'B'
                elif switch == 'B':
                    switch = 'C'
                elif switch == 'C':
                    switch = 'D'
                elif switch == 'D':
                    switch = 'A'
                else:
                    switch = 'A'
                vital_switch_flag = str(switch) + ',' + str(todayDate)
                log.info("switch the recommendation" + vital_switch_flag)
            log.info("After Switch" + switch)
            Variable.set(key="vital_switch_flag", value=vital_switch_flag)
            # engine = create_engine('mysql+pymysql://user:user@123@localhost/zylaapi')  # noqa E303
            # print("starting create vitals job")
            engine = get_data_from_db(db_type="mysql",
                                      conn_id="vital_db")
            # print("got db connection from environment")
            connection = engine.get_conn()
            # print("got the connection no looking for cursor")
            cursor = connection.cursor()
            # print("got the cursor")
            updateweeksqlquery = "UPDATE vital.week_switches set week= '" + switch + "' where id=1"
            cursor.execute(updateweeksqlquery)
            connection.commit()
    except Exception as e:
        log.error(e)
        raise e

