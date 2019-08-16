from common.db_functions import get_data_from_db
import datetime
PAGE_SIZE = 1000

def isRecommended(param, fortoday):
    ret = 0
    day = datetime.datetime.today().weekday()

    if fortoday==False:
        day = 0 if day==6 else day+1

    #elif day == 0:
    #    if param == 5:
    #       ret = 1
    if day == 1:
        if param == 25:
            ret = 1
    #elif day == 2:
    #    if param == 5:
    #        ret = 1
    elif day == 3:
        if param == 26:
            ret = 1
    #elif day == 4:
    #    if param == 5:
    #        ret = 1
    elif day == 5:
        if param in [68, 69, 71, 10, 18]:
            ret = 1
    elif day == 6:
        if param == 5:
            ret = 1

    return ret


def create_vitals():
    try:

        #engine = create_engine('mysql+pymysql://user:user@123@localhost/zylaapi')
        print("starting create vitals job")
        engine = get_data_from_db(db_type="mysql", conn_id="mysql_monolith")
        print("got db connection from environment")
        connection = engine.get_conn()
        print("got the connection no looking for cursor")
        cursor = connection.cursor()
        print("got the cursor")

        cursor.execute("select count(*) from zylaapi.patient_profile where status in (10,4,11,5,18)")
        totalcount = cursor.fetchone()[0]
        print(totalcount)
        numberofPage = int(totalcount / PAGE_SIZE) + 1
        print(numberofPage)
        for i in range(numberofPage):
            patientIdSqlQuerry = "select id from zylaapi.patient_profile where status in (10,4,11,5,18) LIMIT " + str(i * PAGE_SIZE) + ", " + str(PAGE_SIZE)
            cursor.execute(patientIdSqlQuerry)
            patientIdList = []
            patientIdDict = {}
            for row in cursor.fetchall():
                for id in row:
                    patientIdList.append(id)

            print(patientIdList)

            for patientid in patientIdList:
                paramGroupSqlQuery = "select distinct(paramGroupId) from zylaapi.testReadings where patientid = " + str(patientid)
                cursor.execute(paramGroupSqlQuery)
                patientIdParamGroupList = []
                patientIdParamList = []

                for row in cursor.fetchall():
                    for id in row:
                        patientIdParamGroupList.append(id)

                # print(patientIdParamGroupList)

                for paramGroupId in patientIdParamGroupList:
                    paramSqlQuery = "select distinct(paramId) from zylaapi.paramGroupParams where paramGroupId = " + str(paramGroupId)
                    cursor.execute(paramSqlQuery)

                    for row in cursor.fetchall():
                        for id in row:
                            patientIdParamList.append(id)

                # print("Patient Id " + str(patientid))
                # print(patientIdParamList)

                patientIdDict[str(patientid)] = patientIdParamList;

            print(patientIdDict)

            for key, value in patientIdDict.items():
                checkSqlQuery = "select distinct(paramId) from zylaapi.patientTestReadings where forDate=CURDATE() and patientid = " + str(key)
                cursor.execute(checkSqlQuery)
                paramInsertedToday = []
                for row in cursor.fetchall():
                    for id in row:
                        paramInsertedToday.append(id)

                for param in value:
                    recommend = isRecommended(param, True)
                    if param in paramInsertedToday:
                        print("Run Update Query for param " + str(param))
                        # updateSqlQuery = "UPDATE TABLE zylaapi.patientTestReadings SET isRecommended = b'"+ str(recommend) + "' WHERE forDate = CURDATE() and patientid = " + str(key) + " and paramId = " + str(param)
                        # print(updateSqlQuery)
                        # cursor.execute(updateSqlQuery)
                        print(recommend)
                    else:
                        print("Run insert Query for param " + str(param))
                        insertSqlQuery = "INSERT INTO zylaapi.patientTestReadings (patientId, paramId, forDate, isRecommended)  VALUES (" + str(key) + ", " + str(param) + ", CURDATE(), b'" + str(recommend) + "')"
                        print(insertSqlQuery)
                        cursor.execute(insertSqlQuery)
                        print(recommend)

            for key, value in patientIdDict.items():
                checkSqlQuery = "select distinct(paramId) from zylaapi.patientTestReadings where forDate=DATE_ADD(CURDATE(), INTERVAL +1 DAY) and patientid = " + str(key)
                cursor.execute(checkSqlQuery)
                paramInsertedTom = []
                for row in cursor.fetchall():
                    for id in row:
                        paramInsertedTom.append(id)

                for param in value:
                    recommend = isRecommended(param, False)
                    if param in paramInsertedTom:
                        print("Run Update Query for param " + str(param))
                        # updateSqlQuery = "UPDATE TABLE zylaapi.patientTestReadings SET isRecommended = b'" + str(recommend) + "' WHERE forDate = DATE_ADD(CURDATE(), INTERVAL +1 DAY) and patientid = " + str(key) + " and paramId = " + str(param)
                        # print(updateSqlQuery)
                        # cursor.execute(updateSqlQuery)
                        print(recommend)
                    else:
                        print("Run insert Query for param " + str(param))
                        insertSqlQuery = "INSERT INTO zylaapi.patientTestReadings (patientId, paramId, forDate, isRecommended)  VALUES (" + str(key) + ", " + str(param) + ", DATE_ADD(CURDATE(), INTERVAL +1 DAY), b'" + str(recommend) + "')"
                        print(insertSqlQuery)
                        cursor.execute(insertSqlQuery)
                        print(recommend)


    except:
        print("Error Exception raised")
