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

        cursor.execute("select q.patient_id, q.to_status, q.updated_at, r.phoneno, r.countrycode from (select * from "
                       "(select * from zylaapi.patient_status_audit where TIMESTAMPDIFF(minute,updated_at,NOW()) "
                       "Between 15 and 30 group by patient_id Order By updated_at desc limit 1) as p where p.to_status "
                       "in (6,8,9,11) )as q INNER JOIN zylaapi.patient_profile as r ON q.patient_id=r.id")
    
        for row in cursor.fetchall():
            send_event_request(row[0],row[1],row[3],row[4])
        
    except Exception as e:
        print("Error Exception raised")
        print(e) 