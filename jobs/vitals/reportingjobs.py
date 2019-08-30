from airflow.models import Variable
from common.db_functions import get_data_from_db
from config import local_tz

PAGE_SIZE = 1000




def create_reportings_func():
    try:

        vital_reporting_flag = int(Variable.get("vital_reporting_flag", '0'))
        if vital_reporting_flag == 1:
            return
        #engine = create_engine('mysql+pymysql://user:user@123@localhost/zylaapi')
        #print("starting create vitals job")
        engine = get_data_from_db(db_type="mysql", conn_id="mysql_monolith")
        #print("got db connection from environment")
        connection = engine.get_conn()
        #print("got the connection no looking for cursor")
        cursor = connection.cursor()
        #print("got the cursor")

        cursor.execute("select count(*) from zylaapi.patient_profile")
        totalcount = cursor.fetchone()[0]
        #print(totalcount)
        numberofPage = int(totalcount / PAGE_SIZE) + 1
        print(numberofPage)
        for i in range(numberofPage):
            patientIdSqlQuerry = "select id from zylaapi.patient_profile LIMIT " + str(i * PAGE_SIZE) + ", " + str(PAGE_SIZE)
            cursor.execute(patientIdSqlQuerry)
            patientIdList = []
            patientIdDict = {}
            for row in cursor.fetchall():
                for id in row:
                    patientIdList.append(id)

            #print(patientIdList)

            for patientid in patientIdList:
                checkSqlQuery = "select id from zylaapi.reportings where Date(due_date) = curdate() and patient_id = " + str(patientid)
                no_of_rows = cursor.execute(checkSqlQuery)
                if no_of_rows == 0:
                    insertSqlQuery = "INSERT INTO zylaapi.reportings (patient_id, due_date)  VALUES (" + str(patientid) + ", CURRENT_TIMESTAMP())"
                    cursor.execute(insertSqlQuery)


            connection.commit()


    except Exception as e:
        print("Error Exception raised")
        print(e)
