from airflow.models import Variable
from common.db_functions import get_data_from_db
from common.helpers import send_event_request


def broadcast_newuser_whatsapp():

    process_broadcast_newuser_whatsapp = int(Variable.get("process_broadcast_newuser_whatsapp", '0'))
    if process_broadcast_newuser_whatsapp == 1:
        return
    
    try:
        engine = get_data_from_db(db_type="mysql", conn_id="mysql_monolith")
        # print("got db connection from environment")
        connection = engine.get_conn()
        # print("got the connection no looking for cursor")
        cursor = connection.cursor()
        # print("got the cursor")

        cursor.execute("Select id,status,created_at,phoneno,countrycode from zylaapi.patient_profile where status != 4 and TIMESTAMPDIFF(minute,created_at,NOW()) between 15 and 30;")
          
        for row in cursor.fetchall():
            send_event_request(row[0],row[1],row[3],row[4])
        
    except Exception as e:
        print("Error Exception raised")
        print(e) 