import datetime

from airflow.models import Variable
from airflow.utils.log.logging_mixin import LoggingMixin

from common.db_functions import get_data_from_db

PAGE_SIZE = 1000

log = LoggingMixin().log

defVitalGroups = Variable.get("default_vital_groups", deserialize_json=True)



def isRecommendedA(param, fortoday):
    ret = 0
    date = datetime.datetime.today()
    timedelta = datetime.timedelta(hours=5, minutes=30)
    todaydate = date + timedelta
    day = todaydate.weekday()

    if not fortoday:
        day = 0 if day == 6 else day + 1

    # elif day == 0:
    #    if param == 5:
    #       ret = 1
    if day == 1:
        if param == 25:
            ret = 1
    # elif day == 2:
    #    if param == 5:
    #        ret = 1
    elif day == 3:
        if param == 26:
            ret = 1
    # elif day == 4:
    #    if param == 5:
    #        ret = 1
    elif day == 5:
        if param in [1, 2, 10, 18, 61, 68, 69, 71]:
            ret = 1
    elif day == 6:
        if param == 58:
            ret = 1

    return ret


def isRecommendedB(param, fortoday):
    ret = 0
    date = datetime.datetime.today()
    timedelta = datetime.timedelta(hours=5, minutes=30)
    todaydate = date + timedelta
    day = todaydate.weekday()

    if not fortoday:
        day = 0 if day == 6 else day + 1

    # elif day == 0:
    #    if param == 5:
    #       ret = 1
    if day == 1:
            if param == 5:
                ret = 1
    # elif day == 2:
    #    if param == 5:
    #        ret = 1
    elif day == 3:
        if param == 70:
            ret = 1
    # elif day == 4:
    #    if param == 5:
    #        ret = 1
    elif day == 5:
        if param in [10, 18, 68, 69, 71]:
            ret = 1
    elif day == 6:
            ret = 1

    return ret


def isRecommendedC(param, fortoday):
    ret = 0
    date = datetime.datetime.today()
    timedelta = datetime.timedelta(hours=5, minutes=30)
    todaydate = date + timedelta
    day = todaydate.weekday()

    if not fortoday:
        day = 0 if day == 6 else day + 1

    # elif day == 0:
    #    if param == 5:
    #       ret = 1
    if day == 0:
        if param == 5:
            ret = 1
    # elif day == 2:
    #    if param == 5:
    #        ret = 1
    elif day == 3:
        if param == 26:
            ret = 1
    # elif day == 4:
    #    if param == 5:
    #        ret = 1
    elif day == 5:
        if param in [1, 2, 10, 18, 61, 68, 69, 71]:
            ret = 1
    elif day == 6:
        if param == 25:
            ret = 1

    return ret


def isRecommendedD(param, fortoday):
    ret = 0
    date = datetime.datetime.today()
    timedelta = datetime.timedelta(hours=5, minutes=30)
    todaydate = date + timedelta
    day = todaydate.weekday()

    if not fortoday:
        day = 0 if day == 6 else day + 1

    # elif day == 0:
    #    if param == 5:
    #       ret = 1
    if day == 1:
        if param == 70:
            ret = 1
    # elif day == 2:
    #    if param == 5:
    #        ret = 1
    elif day == 3:
        if param == 58:
            ret = 1
    # elif day == 4:
    #    if param == 5:
    #        ret = 1
    elif day == 5:
        if param in [10, 18, 68, 69, 71]:
            ret = 1
    elif day == 6:
        if param == 5:
            ret = 1

    return ret


def create_vitals_func(**kwargs):
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

            if switch == 'A':
                isRecommended = isRecommendedA
            if switch == 'B':
                isRecommended = isRecommendedB
            if switch == 'C':
                isRecommended = isRecommendedC
            if switch == 'D':
                isRecommended = isRecommendedD
            else:
                isRecommended = isRecommendedA

            Variable.set(key="vital_switch_flag", value=vital_switch_flag)
            # engine = create_engine('mysql+pymysql://user:user@123@localhost/zylaapi')  # noqa E303
            # print("starting create vitals job")
            engine = get_data_from_db(db_type="mysql",
                                      conn_id="mysql_monolith")
            # print("got db connection from environment")
            connection = engine.get_conn()
            # print("got the connection no looking for cursor")
            cursor = connection.cursor()
            # print("got the cursor")

            cursor.execute(
                "select count(*) from zylaapi.patient_profile where status in (10,4,11,5,18)")  # noqa E303
            totalcount = cursor.fetchone()[0]
            # print(totalcount)
            numberofPage = int(totalcount / PAGE_SIZE) + 1
            print(numberofPage)
            for i in range(numberofPage):
                patientIdSqlQuerry = "select id from zylaapi.patient_profile where status in (10,4,11,5,18) LIMIT " + str(
                    # noqa E303
                    i * PAGE_SIZE) + ", " + str(PAGE_SIZE)
                cursor.execute(patientIdSqlQuerry)
                patientIdList = []
                patientIdDict = {}
                for row in cursor.fetchall():
                    for id in row:
                        patientIdList.append(id)

                # print(patientIdList)

                for patientid in patientIdList:
                    paramGroupSqlQuery = "select distinct(paramGroupId) from zylaapi.testReadings where patientid = " + str(
                        # noqa E303
                        patientid)
                    numberOfRows = cursor.execute(paramGroupSqlQuery)

                    patientIdParamGroupList = []
                    patientIdParamList = []

                    if (numberOfRows == 0):
                        for defaultId in defVitalGroups:
                            patientIdParamGroupList.append(defaultId)
                            paramInsertQuery = "INSERT INTO " \
                                               "zylaapi.testReadings " \
                                               "(patientId, paramGroupId) " \
                                               "VALUES (" \
                                               + str(patientid) + ", " \
                                               + str(defaultId) + ");"
                            cursor.execute(paramInsertQuery)
                    else:
                        for row in cursor.fetchall():
                            for id in row:
                                patientIdParamGroupList.append(id)

                    # print(patientIdParamGroupList)

                    for paramGroupId in patientIdParamGroupList:
                        paramSqlQuery = "select distinct(paramId) from zylaapi.paramGroupParams where paramGroupId = " + str(
                            # noqa E303
                            paramGroupId)
                        cursor.execute(paramSqlQuery)

                        for row in cursor.fetchall():
                            for id in row:
                                patientIdParamList.append(id)

                    # print("Patient Id " + str(patientid))
                    # print(patientIdParamList)

                    patientIdDict[str(patientid)] = patientIdParamList

                # print(patientIdDict)

                for key, value in patientIdDict.items():
                    checkSqlQuery = "select distinct(paramId) from " \
                                    "zylaapi.patientTestReadings " \
                                    "where forDate=CURDATE() " \
                                    "and patientid = " \
                                    + str(key)
                    cursor.execute(checkSqlQuery)
                    paramInsertedToday = []
                    for row in cursor.fetchall():
                        for id in row:
                            paramInsertedToday.append(id)

                    for param in value:
                        recommend = isRecommended(param, True)
                        if param not in paramInsertedToday:
                            insertSqlQuery = "INSERT INTO zylaapi.patientTestReadings (patientId, paramId, forDate, isRecommended)  VALUES (" + str(
                                # noqa E303
                                key) + ", " \
                                             + str(param) + ", CURDATE(), b'" + str(
                                recommend) + "')"

                            cursor.execute(insertSqlQuery)

                for key, value in patientIdDict.items():
                    checkSqlQuery = "select distinct(paramId) from zylaapi.patientTestReadings where forDate=DATE_ADD(CURDATE(), INTERVAL +1 DAY) and patientid = " + str(
                        # noqa E303
                        key)
                    cursor.execute(checkSqlQuery)
                    paramInsertedTom = []
                    for row in cursor.fetchall():
                        for id in row:
                            paramInsertedTom.append(id)

                    for param in value:
                        recommend = isRecommended(param, False)
                        if param not in paramInsertedTom:
                            insertSqlQuery = "INSERT INTO zylaapi.patientTestReadings (patientId, paramId, forDate, isRecommended)  VALUES (" + str(
                                # noqa E303
                                key) + ", " + str(
                                param) + ", DATE_ADD(CURDATE(), INTERVAL +1 DAY), b'" + str(  # noqa E303
                                recommend) + "')"

                            cursor.execute(insertSqlQuery)

                connection.commit()
    except Exception as e:
        log.error(e)
        raise e
