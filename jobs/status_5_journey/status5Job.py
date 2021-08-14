from common.db_functions import get_data_from_db
from airflow.models import Variable
from airflow.utils.log.logging_mixin import LoggingMixin
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail
from common.http_functions import make_http_request


log = LoggingMixin().log

template_ids = {
    '1': 'd-42df7dfd41514b4c96e869382edfc8b3',
    '2': 'd-42df7dfd41514b4c96e869382edfc8b3',
    '3': 'd-53ac16b96d3d479d9c5412ff8a8fc262',
    '4': 'd-53ac16b96d3d479d9c5412ff8a8fc262',
    '5': 'd-a0b53cab47194d70b43223f7dd211c48',
    '6': 'd-a0b53cab47194d70b43223f7dd211c48'
}


def getPatientStatus():
    try:
        engine = get_data_from_db(db_type="mysql", conn_id="mysql_monolith")
        connection = engine.get_conn()
        cursor = connection.cursor()

        cursor.execute('''
            SELECT 
                u.patient_id, u.to_status, u.updated_at, pp.email, pp.firstName,
                CASE
                    WHEN DATEDIFF(CURRENT_DATE(), u.updated_at) = 1 THEN '1'
                    WHEN (DATEDIFF(CURRENT_DATE(), u.updated_at) = 6) THEN '2'
                    WHEN (DATEDIFF(CURRENT_DATE(), u.updated_at) = 16) THEN '3'
                    WHEN (DATEDIFF(CURRENT_DATE(), u.updated_at) = 25) THEN '4'
                    WHEN (DATEDIFF(CURRENT_DATE(), u.updated_at) = 35) THEN '5'
                    WHEN (DATEDIFF(CURRENT_DATE(), u.updated_at) = 45) THEN '6'
                    ELSE '0'
                END AS Tag
            FROM
                (SELECT 
                    patient_id, MAX(id) AS mid
                FROM
                    zylaapi.patient_status_audit
                GROUP BY patient_id) mu
                    INNER JOIN
                zylaapi.patient_status_audit u ON mu.mid = u.id
                    LEFT OUTER JOIN
                zylaapi.patient_profile pp ON u.patient_id = pp.id where u.to_status=5 and email != ''
        ''')

        patients = []
        
        for row in cursor.fetchall():
            if row[5] != '0':
                log.info(row)
                patients.append(row)

        return patients

    except Exception as e:
        log.info("Error Exception raised")
        log.info(e)


def validateEmail(email):
    try:
        query_endpoint = "?api=6113abfe2f5c1&email="+email
        query_status, query_data = make_http_request(conn_id="http_debounce",
                                                        endpoint=query_endpoint, method="GET")
        log.info(query_data["debounce"]["result"])
        if (query_data["debounce"]["result"] == "Safe to Send"):
            return True
        else:
            return False
    except:
        log.error("somethign went wrong ", email)


def sendMail(email, name, template_id):
    if(validateEmail(email)):
        sg = SendGridAPIClient(Variable.get('SENDGRID_API_KEY'))
        data = {
            "from":{
                "email":"care@zyla.in",
                "name" : "Zyla Health"
            },
            "personalizations":[
                {
                    "to":[
                        {
                        "email":email
                        }
                    ],
                    "dynamic_template_data":{
                    "firstName":name
                    }
                }
            ],
            "template_id":template_id,
            "asm": {
                "group_id": 15755,
                "groups_to_display": [
                15755,15843
                ]
            }
        }
        try:
            response = sg.client.mail.send.post(request_body=data)
        except Exception as e:
            log.info("Couldn't send the mail")
            log.info(e)
            
    else:
        log.error("Email got debounced ", email)

def initializer(**kwargs):
    patients = getPatientStatus()
    log.info(patients)
    for p in patients:
        sendMail(p[3],p[4], template_ids[p[5]])
    
