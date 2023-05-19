from airflow.models import Variable
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.contrib.hooks.mongo_hook import MongoHook
from pymongo.collection import Collection
from pymongo.cursor import Cursor
from common.db_functions import get_data_from_db
from common.helpers import process_custom_message_user_id
from datetime import datetime


log = LoggingMixin().log


def get_patient_ids():
    try:
        mongo_conn = MongoHook(conn_id="mongo_prod").get_conn()
        collection = mongo_conn.get_database(
            "tracking").get_collection("md_tracking")
        medicines = ["MED98",	"MED97",	"MED114",	"MED591",	"MED590",	"MED637",	"MED2170",	"MED2157",	"MED2237",	"MED2169",	"MED2168",	"MED2161",	"MED2171",	"MED2167",	"MED2158",	"MED2238",	"MED2236",	"MED2756",	"MED2674",	"MED2677",	"MED2673",	"MED2678",	"MED2755",	"MED2819",	"MED2818",	"MED2958",	"MED2947",	"MED2951",	"MED2923",	"MED2994",	"MED2975",	"MED2976",	"MED2928",	"MED2949",	"MED2998",	"MED2959",	"MED2924",	"MED2974",	"MED3017",	"MED3043",	"MED3003",	"MED3027",	"MED3044",	"MED3005",	"MED3026",	"MED3018",	"MED3029",	"MED3006",	"MED3004",	"MED3172",	"MED3171",	"MED3455",	"MED3454",	"MED3506",	"MED3453",	"MED3456",	"MED3443",	"MED3505",	"MED4203",	"MED4202",	"MED4762",	"MED4763",	"MED4801",	"MED4802",	"MED5173",	"MED5174",	"MED5171",	"MED5176",	"MED5295",	"MED5287",	"MED5298",	"MED5299",	"MED5296",	"MED5318",	"MED5367",	"MED5368",	"MED5786",	"MED5700",	"MED5701",	"MED5787",	"MED5784",	"MED5785",	"MED5788",	"MED6094",	"MED6095",	"MED6479",	"MED6520",	"MED6509",	"MED6505",	"MED6501",	"MED6507",	"MED6506",	"MED6502",	"MED6733",	"MED6749",	"MED6728",	"MED6729",	"MED6739",	"MED6510",	"MED6508",	"MED6504",	"MED6740",	"MED6864",	"MED6900",	"MED6892",	"MED6895",	"MED6898",	"MED6897",	"MED6899",	"MED6967",	"MED6977",	"MED6909",	"MED6918",	"MED6913",	"MED6863",	"MED6859",	"MED6861",	"MED6867",	"MED6869",	"MED6877",	"MED6866",	"MED6886",	"MED6876",	"MED6868",	"MED6888",	"MED6882",	"MED6890",	"MED6893",	"MED6884",	"MED6925",	"MED6904",	"MED6911",	"MED6908",	"MED6480",	"MED6478",	"MED6511",	"MED6730",	"MED6731",	"MED6715",	"MED6717",	"MED6727",	"MED6732",	"MED6880",	"MED6885",	"MED6879",	"MED6875",	"MED6871",	"MED6872",	"MED6896",	"MED6917",	"MED6914",	"MED6916",	"MED6920",	"MED6923",	"MED6901",	"MED6924",	"MED6718",	"MED6850",	"MED6862",	"MED6848",	"MED6849",	"MED6860",	"MED6887",	"MED6883",	"MED6878",	"MED6891",	"MED6870",	"MED6873",	"MED6881",	"MED6889",	"MED6874",	"MED6902",	"MED6972",	"MED6912",	"MED6905",	"MED6921",	"MED6903",	"MED6910",	"MED6922",	"MED6973",	"MED6907",	"MED6906",	"MED6978",	"MED7323",	"MED7324",	"MED7370",	"MED7458",	"MED7681",	"MED7782",	"MED7778",	"MED7777",	"MED7781",	"MED8657",	"MED8659",	"MED8658",	"MED8660",	"MED8655",	"MED8654",	"MED8656",	"MED8535",	"MED8661",	"MED9737",	"MED9727",	"MED10257",	"MED10419",	"MED10420",	"MED10403",	"MED10423",	"MED10425",	"MED10424",	"MED10422",	"MED10402",	"MED10986",	"MED10421",	"MED13709",	"MED14957",	"MED14960",	"MED14959",	"MED15424",	"MED16371",	"MED16372",	"MED16448",	"MED17050",	"MED17373",	"MED17408",	"MED17374",	"MED17411",	"MED17757",	"MED17755",	"MED17756",	"MED17754",	"MED17759",	"MED17410",	"MED17787",	"MED18836",	"MED18837",	"MED18855",	"MED18863",	"MED18862",	"MED19032",	"MED19353",	"MED20133",	"MED21606",	"MED21607",	"MED22680",	"MED22695",	"MED22697",	"MED22676",	"MED22679",	"MED22696",	"MED22677",	"MED22694",	"MED22829",	"MED22678",	"MED22681",	"MED22827",	"MED22949",	"MED22830",	"MED22828",	"MED22948",	"MED25029",	"MED25028",	"MED25155",	"MED25177",	"MED25172",	"MED25167",	"MED25153",	"MED25156",	"MED25158",	"MED25154",	"MED25173",	"MED25178",	"MED25152",	"MED25159",	"MED25171",	"MED25174",	"MED25157",	"MED25176",	"MED25175",	"MED25634",	"MED25635",	"MED25631",	"MED25637",	"MED25632",	"MED25636",	"MED25633",	"MED26233",	"MED26232",	"MED26541",	"MED26542",	"MED26534",	"MED26562",	"MED26535",	"MED26561",	"MED26786",	"MED26787",	"MED26629",	"MED27017",	"MED27018",	"MED27057",	"MED27056",	"MED27457",	"MED27456",	"MED27448",	"MED27497",	"MED27498",	"MED27542",	"MED27547",	"MED27546",	"MED27596",	"MED27586",	"MED27654",	"MED27662",	"MED27652",	"MED27621",	"MED27724",	"MED27733",	"MED27776",	"MED27449",	"MED27474",	"MED27475",	"MED27554",	"MED27509",	"MED27540",	"MED27593",	"MED27599",	"MED27570",	"MED27620",	"MED27605",	"MED27631",	"MED27632",	"MED27668",	"MED27698",	"MED27696",	"MED27751",	"MED27510",	"MED27541",	"MED27563",	"MED27505",	"MED27561",	"MED27598",	"MED27600",	"MED27573",	"MED27592",	"MED27622",	"MED27628",	"MED27604",	"MED27638",	"MED27630",	"MED27629",	"MED27644",	"MED27613",	"MED27663",	"MED27699",	"MED27704",	"MED27756",	"MED27736",	"MED27767",	"MED27666",	"MED27667",	"MED27725",	"MED27761",	"MED27760",	"MED27777",	"MED27834",	"MED27459",	"MED27486",	"MED27499",	"MED27504",	"MED27585",	"MED27576",	"MED27597",	"MED27571",	"MED27574",	"MED27615",	"MED27608",	"MED27648",	"MED27637",	"MED27669",	"MED27697",	"MED27750",	"MED27757",	"MED27766",	"MED27835",	"MED29195",	"MED29196",	"MED29349",	"MED29293",	"MED29292",	"MED29285",	"MED29286",	"MED29560",	"MED29561",	"MED29714",	"MED29713",	"MED29715",	"MED30588",	"MED30531",	"MED30532",	"MED30589",	"MED30991",	"MED31111",	"MED31445",	"MED31446",	"MED31649",	"MED31860",	"MED31861",	"MED32242",	"MED32941",	"MED33385",	"MED34033",	"MED34095",	"MED34096",	"MED35087",	"MED37614",	"MED39548",	"MED39677",	"MED39570",	"MED40492",	"MED41315",	"MED41316",	"MED41470",	"MED41469",	"MED42524",	"MED44057",	"MED45082","MED3242",	"MED3241",	"MED4500",	"MED4504",	"MED4505",	"MED30728",	"MED30717",	"MED30729",	"MED30724",	"MED30730",	"MED30727",	"MED30722",	"MED30723",	"MED30725",	"MED30726",	"MED30739",	"MED31114",	"MED35998",	"MED38042",	"MED41472",	"MED41473",	"MED46424"]
        
        results = collection.find(
            {{"medicineDetails.medicine": {"$in": medicines}}})
        patientIds = []
        for q in results:
            patientIds.append(q['patientId'])

        engine = get_data_from_db(db_type="mysql", conn_id="mysql_monolith")
        connection = engine.get_conn()
        cursor = connection.cursor()
        filter_active_patient_query = "select id from patient_profile where status=4 and new_chat=1 and id in (" + ','.join(
            str(x) for x in patientIds) + ")"

        cursor.execute(filter_active_patient_query)
        activePatientIds = []
        for row in cursor.fetchall():
            activePatientIds.append(row[0])

        return activePatientIds
    except Exception as e:
        log.info("Error Exception raised")
        log.info(e)


def broadcast_active_hh_medicine():
    patientIds = get_patient_ids()
    print(patientIds)
    process_broadcast_active = int(Variable.get("process_broadcast_active_hh_medicine",
                                                '1'))
    if process_broadcast_active == 1:
        return

    date_string = f'{datetime.now():%Y-%m-%d %H:%M:%S%z}'
    group_id = "broadcast_active_hh_medicine" + date_string

    message = str(Variable.get("broadcast_active_hh_medicine_msg", ""))
    engine = get_data_from_db(db_type="mysql", conn_id="mysql_monolith")
    connection = engine.get_conn()
    cursor = connection.cursor()
    for patient_id in patientIds:
        if message:
            try:

                user_id_sql_query = "select pp.id from zylaapi.patient_profile as pp " \
                                    "left join zylaapi.doc_profile as dp" \
                                    "on pp.referred_by = dp.id" \
                                    " where pp.id = " + str(patient_id) + " and (dp.code like 'HH%' or dp.code like 'AZ%'); "
                cursor.execute(user_id_sql_query)
                user_id = 0
                for row in cursor.fetchall():
                    user_id = row[0]
                log.info("sending for user id " + str(user_id))
                if user_id != 0:
                    process_custom_message_user_id(user_id, message, "", group_id)
            except Exception as e:
                print("Error Exception raised")
                print(e)
