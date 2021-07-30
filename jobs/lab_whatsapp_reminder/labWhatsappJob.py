from airflow.models import Variable
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.contrib.hooks.mongo_hook import MongoHook
from pymongo.collection import Collection
from pymongo.cursor import Cursor
from datetime import datetime, timedelta
from common.http_functions import make_http_request


log = LoggingMixin().log


def getBookings():
    try:
        mongo_conn = MongoHook(conn_id="mongo_prod").get_conn()
        collection = mongo_conn.get_database(
            "labtest_request_service").get_collection("booking")
        d = datetime.now() + timedelta(days=1)
        dString = d.strftime("%Y-%m-%d")
        results = collection.find({"preferreddate": dString, "disposition": {"$in": ["Test Booked/Postpaid", "Test Booked/Prepaid - On Call", "Test Booked/Prepaid - Website"]} })
        phoneNumbers = []

        print(results)

        for q in results:
            phoneNumbers.append(q['phoneno'])
            log.info(q)

        return phoneNumbers
    except Exception as e:
        log.info("Error Exception raised")
        log.info(e)


def initializer(**kwargs):
    try:
        phoneNumbers = getBookings()
        for ph in phoneNumbers:
            status, body = make_http_request(
                    conn_id="interakt_event",
                    endpoint="", method="POST", payload={"phoneNumber": ph, "countryCode": "+91", "Event":"lab test one day past reminder"})
    except Exception as e:
        raise ValueError(str(e))
