from common.db_functions import get_data_from_db
from airflow.models import Variable
from airflow.utils.log.logging_mixin import LoggingMixin
from jobs.status_5_journey.status5Job import validateEmail
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail
from common.http_functions import make_http_request
from datetime import datetime, timedelta
import json
import pandas as pd
import math



log = LoggingMixin().log

def getRegistrants():
    now = datetime.now()
    now = now + timedelta(days=7)
    fr = now + timedelta(days=1)
    to = now + timedelta(days=2)
    zoom_ids = []
    try:
        query_endpoint = "webinars?starting_gt=" + fr.strftime("%Y-%m-%d") + "&starting_lt=" + to.strftime("%Y-%m-%d")
        query_status, query_data = make_http_request(conn_id="http_strapi",
                                                        endpoint=query_endpoint, method="GET")
        if query_status == 200:
            for w in query_data:
                zoom_ids.append(w['zoom_id'])
    except:
        log.error("Couldn't fetch the webinars from strapi")
    
    if len(zoom_ids) > 0:
        payload = {"page_size" : 300}
        query_endpoint = zoom_ids[0] + "/registrants?page_size=300"
        query_status, query_data = make_http_request(conn_id="http_zoom_api", endpoint=query_endpoint, method="GET")

        rdf = pd.DataFrame(query_data['registrants'])
        registrant_df = pd.DataFrame([rdf])
        iterations = math.ceil(query_data['total_records']/300)
        next_token = query_data['next_page_token']

        for i in range(iterations-1):
            new_query_endpoint =  query_endpoint + "&next_page_token=" + next_token
            query_status, query_data = make_http_request(conn_id="http_zoom_api", endpoint=new_query_endpoint, method="GET")

            rdf = pd.DataFrame(query_data['registrants'])
            registrant_df = pd.DataFrame([registrant_df, rdf])
            next_token = query_data['next_page_token']
        
        return registrant_df
    
    else:
        log.info("No webinars")
        return -1

        


def sendMail(email, emailVars):
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
                    "dynamic_template_data": emailVars
                }
            ],
            "template_id":"d-8457515b14cd456fb13b4158680e22b6",
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
    registrants = getRegistrants()
    if registrants != -1:
        log.info(registrant_df)