from airflow.models import Variable
from common.db_functions import get_data_from_db
import datetime
from config import local_tz

PAGE_SIZE = 1000



def create_meditation_metrics():
    try:

        meditation_create_flag = int(Variable.get("meditation_create_flag", '0'))
        if meditation_create_flag == 1:
            return
        #engine = create_engine('mysql+pymysql://user:user@123@localhost/zylaapi')

        engine = get_data_from_db(db_type="mysql", conn_id="mysql_monolith")

        connection = engine.get_conn()

        cursor = connection.cursor()


        cursor.execute("select count(distinct patientId) from zylaapi.meditationLogs where forDate < CURDATE() and forDate > DATE_SUB(CURDATE(), INTERVAL 2 DAY) and startEnd = 1")
        totalcount = cursor.fetchone()[0]
        #print(totalcount)
        numberofPage = int(totalcount / PAGE_SIZE) + 1
        print(numberofPage)
        for i in range(numberofPage):
            patientIdSqlQuerry = "select distinct patientId from zylaapi.meditationLogs where forDate < CURDATE() and forDate > DATE_SUB(CURDATE(), INTERVAL 2 DAY) and startEnd = 1 LIMIT " + str(i * PAGE_SIZE) + ", " + str(PAGE_SIZE)
            rowcount = cursor.execute(patientIdSqlQuerry)
            patientIdList = []

            if rowcount > 0:

                for row in cursor.fetchall():
                    for id in row:
                        patientIdList.append(id)


                for patientid in patientIdList:
                    metricssqlQuery = "select dailyMetrics, MaxMetrics from zylaapi.meditationMetrics where patientId = " + str(patientid)
                    metricsrowcount = cursor.execute(metricssqlQuery)

                    if metricsrowcount > 0:
                        for row in cursor.fetchall():
                            dailycount = int(row[0]) + 1
                            maxcount = int(row[1])
                            if maxcount < dailycount:
                                maxcount = dailycount

                            updateSqlQuery = "UPDATE zylaapi.meditationMetrics SET dailyMetrics = " + str(
                                dailycount) +", maxMetrics = " + str(maxcount) + " where patientId = " + str(patientid)
                            # print(updateSqlQuery)
                            cursor.execute(updateSqlQuery)


                    else:
                        insertSqlQuery = "INSERT INTO zylaapi.meditationMetrics (patientId, dailyMetrics, maxMetrics)  VALUES (" + str(
                            patientid) + ", 1, 1)"

                        cursor.execute(insertSqlQuery)

        cursor.execute("select count(*) from zylaapi.patient_profile where id not in (select distinct patientId from zylaapi.meditationLogs where forDate < CURDATE() and forDate > DATE_SUB(CURDATE(), INTERVAL 2 DAY) and startEnd = 1)")
        totalcount = cursor.fetchone()[0]
        # print(totalcount)
        numberofPage = int(totalcount / PAGE_SIZE) + 1
        print(numberofPage)
        for i in range(numberofPage):
            patientIdSqlQuerry = "select id from zylaapi.patient_profile where id not in (select distinct patientId from zylaapi.meditationLogs where forDate < CURDATE() and forDate > DATE_SUB(CURDATE(), INTERVAL 2 DAY) and startEnd = 1) LIMIT " + str(
                i * PAGE_SIZE) + ", " + str(PAGE_SIZE)
            rowcount = cursor.execute(patientIdSqlQuerry)
            patientIdList = []

            if rowcount > 0:

                for row in cursor.fetchall():
                    for id in row:
                        patientIdList.append(id)

                for patientid in patientIdList:
                    metricssqlQuery = "select dailyMetrics, MaxMetrics from zylaapi.meditationMetrics where patientId = " + str(
                        patientid)
                    metricsrowcount = cursor.execute(metricssqlQuery)

                    if metricsrowcount > 0:
                        for row in cursor.fetchall():
                            dailycount = int(row[0]) + 1
                            maxcount = int(row[1])
                            if maxcount < dailycount:
                                maxcount = dailycount

                            updateSqlQuery = "UPDATE zylaapi.meditationMetrics SET dailyMetrics = 0 where patientId = " + str(patientid)
                            # print(updateSqlQuery)
                            cursor.execute(updateSqlQuery)


                    else:
                        insertSqlQuery = "INSERT INTO zylaapi.meditationMetrics (patientId, dailyMetrics, maxMetrics)  VALUES (" + str(
                            patientid) + ", 0, 0)"

                        cursor.execute(insertSqlQuery)



        connection.commit()




    except Exception as e:
        print("Error Exception raised")
        print(e)
