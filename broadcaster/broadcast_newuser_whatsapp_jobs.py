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

        cursor.execute("Select p.patient_id,p.to_status,p.time, r.phoneno,r.countrycode from "
                       "(select * from (select patient_id , to_status , case when TIMESTAMPDIFF"
                       "(minute,MAX(updated_at),NOW()) Between 15 and 30 then 'T' ELSE 'F' END as time , "
                       "max(updated_at) FROM zylaapi.patient_status_audit where to_status in (6,8,9,11) "
                       "group by 1 order by updated_at desc) as q where q.time ='T') as p "
                       "INNER JOIN zylaapi.patient_profile as r ON p.patient_id=r.id")
    
        for row in cursor.fetchall():
            send_event_request(row[0],row[1],row[3],row[4])
        
    except Exception as e:
        print("Error Exception raised")
        print(e) 