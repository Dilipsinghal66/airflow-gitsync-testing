from airflow.models import Variable
from common.db_functions import get_data_from_db
from common.helpers import send_chat_message_patient_id
from airflow.contrib.hooks.mongo_hook import MongoHook
from pymongo.collection import Collection
from pymongo.cursor import Cursor

def broadcast_cc_algo():
    process_broadcast_cc_algo = int(
        Variable.get("process_broadcast_cc_algo", '0'))
    if process_broadcast_cc_algo == 1:
        return

    try:
        engine = get_data_from_db(db_type="mysql", conn_id="mysql_monolith")
        # print("got db connection from environment")
        connection = engine.get_conn()
        # print("got the connection no looking for cursor")
        cursor = connection.cursor()
        # print("got the cursor")

        cursor.execute("Select id,gender,lastName from zylaapi.patient_profile where id in (1774, 71519, 1652, 69730, 92161)")

        for row in cursor.fetchall():
            icds=latest_cc(row[0])
            if not icds:
                continue
            else:
                cc=get_common_name(icds)
                msg=form_msg(row[1],row[2],cc)
                payload_dynamic = {
                    "action": "dynamic_message",
                    "message": msg
                }
                send_chat_message_patient_id(row[0],payload_dynamic)
    except Exception as e:
        print("Error Exception raised")
        print(e)

def latest_cc(patient_id ):
    try:
        mongo_conn = MongoHook(conn_id="mongo_prod").get_conn()
        collection = mongo_conn.get_database(
            "tracking").get_collection("cc_tracking")

        results = collection.find({"patientId": patient_id},{"_id":0,"chiefComplaints.ongoing":1,"chiefComplaints.complaint":1}).sort("dateCreated",-1).limit(1)
        icds =[]
        if results is not None:
            for q in results[0]['chiefComplaints']:
                if q['ongoing']:
                    icds.append(q['complaint'])
        return icds
    except Exception as e:
        print("Error Exception raised")
        print(e)


def get_common_name(icds):
    try:
        engine = get_data_from_db(db_type="mysql", conn_id="mysql_datatable")
        # print("got db connection from environment")
        connection = engine.get_conn()
        # print("got the connection no looking for cursor")
        cursor = connection.cursor()
        # print("got the cursor")
        icd_string=""
        for x in icds:
            icd_string += "'"+x+"'"+","
        icd_string=icd_string[:-1]
        cursor.execute("SELECT disease_chief_complaint,common_terms,icd_code FROM datatable.icds where icd_code IN  (" + icd_string+ ")")
        cc = []
        for row in cursor.fetchall():
            if row[1] is None:
                cc.append(row[0])
            else:
                cc.append(row[1])
        return cc
    except Exception as e:
        print("Error Exception raised")
        print(e)

def form_msg(salutation,surname,cc):
    salut = ''
    if salutation==1:
        salut="Ms"
    elif salutation==2:
        salut="Mr"

    msg = "Dear "+salut+" "+surname+" - The doctors would like to know how you are doing on the below health issues, please let me know which are better and which are same as before:"
    for c in cc:
            msg +="<br>"+"⬤"+" "+c+" "
    return msg