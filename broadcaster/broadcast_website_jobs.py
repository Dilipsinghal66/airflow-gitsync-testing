from airflow.models import Variable
from airflow.utils.log.logging_mixin import LoggingMixin
from common.db_functions import get_data_from_db
from common.helpers import fcm_message_send

log = LoggingMixin().log

PAGE_SIZE = 5000

def broadcast_website():
    process_broadcast_website = int(
        Variable.get("process_broadcast_website", '0'))
    if process_broadcast_website == 1:
        return

    try:
        engine = get_data_from_db(db_type="mysql", conn_id="mysql_monolith")
        # print("got db connection from environment")
        connection = engine.get_conn()
        # print("got the connection no looking for cursor")
        cursor = connection.cursor()
        # print("got the cursor")

        cursor.execute("select count(distinct(fire_base_uid)) from ZylaWebsite.notifications "
                       "where fire_base_uid <> ''")
        totalcount = cursor.fetchone()[0]
        # print(totalcount)
        numberofPage = int(totalcount / PAGE_SIZE) + 1
        # print(numberofPage)

        message = str(Variable.get("broadcast_website_msg", ''))
        title = str(Variable.get("broadcast_website_title", ''))

        for i in range(numberofPage):
            firebase_id_query = "select distinct(fire_base_uid) from ZylaWebsite.notifications where " \
                                "fire_base_uid <> '' LIMIT " + str(i * PAGE_SIZE) + ", " + str(PAGE_SIZE)
            cursor.execute(firebase_id_query)
            firebase_id_list = []

            for row in cursor.fetchall():
                firebase_id_list.append(row[0])

            print(firebase_id_list)

            fcm_message_send(firebase_id_list, message, title)

    except Exception as e:
        print("Error Exception raised")
        print(e)
