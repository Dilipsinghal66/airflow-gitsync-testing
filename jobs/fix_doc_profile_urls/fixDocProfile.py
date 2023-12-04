from common.db_functions import get_data_from_db
from airflow.models import Variable

PAGE_SIZE = 1000
S3_URL = 'https://az-doc.s3.ap-south-1.amazonaws.com/'
S3_URL_PFIZER = 'https://pfizer-pihu.s3.ap-south-1.amazonaws.com/images/'

def fix_doc_profile_url():
    try:
        engine = get_data_from_db(db_type="mysql", conn_id="mysql_monolith")
        # print("got db connection from environment")
        connection = engine.get_conn()
        # print("got the connection no looking for cursor")
        cursor = connection.cursor()
        # print("got the cursor")

        cursor.execute("select count(*) from zylaapi.doc_profile where code like '%AZ%' or code like '%ZH%' or code like 'HH%' or code like 'NA%' or code like 'ND%'")
        totalcount = cursor.fetchone()[0]
        # print(totalcount)
        numberofPage = int(totalcount / PAGE_SIZE) + 1
        print(numberofPage)
        for i in range(numberofPage):
            docIdQuery = "select id from zylaapi.doc_profile where code like '%ZH%' or code like 'HH%' or code like 'AZ%' or code like 'NA%' or code like 'ND%' and id != 20 LIMIT " + str(  # noqa E303
                i * PAGE_SIZE) + ", " + str(PAGE_SIZE)
            docCodeQuery  = "select code from zylaapi.doc_profile where code like '%ZH%' or code like 'HH%' or code like 'AZ%' or code like 'NA%' or code like 'ND%' and id != 20 LIMIT " + str(  # noqa E303
                i * PAGE_SIZE) + ", " + str(PAGE_SIZE)
            cursor.execute(docIdQuery)
            docIdList = []
            for row in cursor.fetchall():
                for id in row:
                    docIdList.append(id)

            cursor.execute(docCodeQuery)
            docCodeList = []
            for row in cursor.fetchall():
                for code in row:
                    docCodeList.append(code)

            # print(patientIdList)

            for docId, docCode in zip(docIdList, docCodeList):
                if docCode[:2] == "NA" or docCode[:2] == "ND":
                    docUpdateQuery = "update zylaapi.doc_profile set profile_image='" + S3_URL_PFIZER + docCode + ".jpg'" + " where id=" + str(docId)
                    cursor.execute(docUpdateQuery)
                else:
                    docUpdateQuery = "update zylaapi.doc_profile set profile_image='" + S3_URL + docCode + ".jpg'" + " where id=" + str(docId)
                    cursor.execute(docUpdateQuery)

            connection.commit()
    except Exception as e:
        print("Error Exception raised")
        print(e)
