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

# Type 1 — Today
# Type 2 — Tomorrow

def getRegistrants(rType):
    now = datetime.now()
    if rType == 1:
        fr = now + timedelta(days=0)
        to = now + timedelta(days=1)
    else:
        fr = now + timedelta(days=1)
        to = now + timedelta(days=2)
    zoom_ids = []
    webinar = ""
    try:
        query_endpoint = "webinars?starting_gt=" + fr.strftime("%Y-%m-%d") + "&starting_lt=" + to.strftime("%Y-%m-%d")
        query_status, query_data = make_http_request(conn_id="http_strapi",
                                                        endpoint=query_endpoint, method="GET")
        if query_status == 200:
            for w in query_data:
                zoom_ids.append(w['zoom_id'])
            if len(query_data) > 0:
                webinar = query_data[0]
    except:
        log.error("Couldn't fetch the webinars from strapi")
    
    if len(zoom_ids) > 0:
        query_endpoint = zoom_ids[0] + "/registrants?page_size=300"
        query_status, query_data = make_http_request(conn_id="http_zoom_api", endpoint=query_endpoint, method="GET")
        print(query_status)
        rdf = pd.DataFrame(query_data['registrants'])
        registrant_df = pd.DataFrame(rdf)
        iterations = math.ceil(query_data['total_records']/300)
        next_token = query_data['next_page_token']

        for i in range(iterations-1):
            new_query_endpoint =  query_endpoint + "&next_page_token=" + next_token
            query_status, query_data = make_http_request(conn_id="http_zoom_api", endpoint=new_query_endpoint, method="GET")
            print(query_data)
            rdf = pd.DataFrame(query_data['registrants'])
            registrant_df = pd.concat([registrant_df, rdf])
            next_token = query_data['next_page_token']
        
        return registrant_df, webinar
    
    else:
        log.info("No webinars")
        return -1,-1

        


def sendMail(emailFrom,emailTo, emailVars):
    if(validateEmail(emailTo)):
        sg = SendGridAPIClient(Variable.get('SENDGRID_API_KEY'))
        data = {
            "from":{
                "email": emailFrom,
                "name" : "Zyla Health"
            },
            "personalizations":[
                {
                    "to":[
                        {
                        "email":emailTo
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
        log.error("Email got debounced ", emailTo)

def initializer(**kwargs):
    rType = kwargs['rType']
    if rType == 1:
        datestr = datetime.now().strftime("%b %d, %Y") + " at 7 PM"
        timeStr = "7 PM today"
    else:
        ds = datetime.now() + timedelta(days=1)
        datestr = ds.strftime("%b %d, %Y") + " at 7 PM"
        timeStr = "7 PM tomorrow"

    registrants,webinar = getRegistrants(rType)
    if webinar != -1:
        log.info(registrants)
        for i, row in registrants.iterrows():
            ob = {"time": timeStr, "topic": webinar['title'],"firstName": row['first_name'], "date_time":datestr,"join_url": row['join_url']}
            print(ob)
            sendMail("noreply@mail.zyla.in", row['email'], ob)


def interaktJob(**kwargs):
    registrants,webinar = getRegistrants(1)
    if webinar != -1:
        log.info(registrants)
        for i, row in registrants.iterrows():
            if row['phone'] != "":
                ob = {"phoneNumber": ph, "countryCode": "+91", "traits": { "name": row['first_name'], "email": row['email'], "Join_url":  row['join_url'].replace("https://us06web.zoom.us/",""), "Tags": "webinar_" + webinar['zoom_id']}}
                status, body = make_http_request(
                    conn_id="interakt_user",
                    endpoint="", method="POST", payload=ob)
                print("Creating interkat for ", ob)
                print(status)

        print("Interakt users created")
            