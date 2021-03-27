from airflow.models import Variable
from common.db_functions import get_data_from_db
from airflow.utils.log.logging_mixin import LoggingMixin
from common.helpers import get_pa_details
from common.helpers import process_custom_message

log = LoggingMixin().log

rules = {"days": [
    {"day": 1, "question": 1, "answer": "Diabetes" , "Default": 6771041317325500416, "No": 6771038651551707136},
    {"day": 2, "Default": 6771045771680542720},
    {"day": 3, "question": 1, "answer": "Blood pressure" , "Default": 6771042703534583808, "No": 6771043383425617920},
    {"day": 4, "Default": 6771044794772615168},
    {"day": 5, "question": 7, "answer": "High uric acid", "Default": 6771049551725719552, "No": 6771048371847135232},
    {"day": 6, "Default": 6771049959506202624},
    {"day": 7, "question": 1, "answer": "Kidney health", "Default": 6771053924802088960, "No": 6771053519570857984},
    {"day": 8, "Default": 6771054175230513152},
    {"day": 9, "question": 1, "answer": "Liver health", "Default": 6771055213455855616, "No": 6771054906231476224},
    {"day": 10, "Default": 6771055628135661568},
    {"day": 11, "question": 23, "answer": "Constipation", "Default": 6771057005346410496, "No": 6771056138538962944},
    {"day": 12, "Default": 6771057853662724096},
    {"day": 13, "question": 2, "answer": "Thyroid problems", "Default": 6771058903043928064, "No": 6771058517333397504},
    {"day": 14, "Default": 6771059929077403648},
    {"day": 15, "question": 23, "answer": "Constipation", "Default": 6771065805257699328, "No": 6771064087373303808},
    {"day": 16, "Default": 6771070429521661952},
    {"day": 17, "question": 1, "answer": "PCOD", "Default": 6771072441393315840, "No": 6771071716483366912},
    {"day": 18, "Default": 6771072907856203776},
    {"day": 19, "question": 24, "answer": "Yes", "Default": 6771074500602089472, "No": 6771073479451758592},
    {"day": 20, "Default": 6771078388551380992},
    {"day": 21, "question": 1, "answer": "Heart health", "Default": 6771084178108678144, "No": 6771082482814701568},
    {"day": 22, "Default": 6771085884672954368},
    {"day": 23, "question": 12, "answer": "Cholesterol/ Triglycerides", "Default": 6771088083037704192, "No":
        6771086573008572416},
    {"day": 24, "Default": 6771089534037651456},
    {"day": 25, "question": 7, "answer": "Low hemoglobin", "Default": 6771091564374388736, "No": 6771090232617619456},
    {"day": 26, "Default": 6771092662398898176},
    {"day": 27, "question": 7, "answer": "Low Vitamin D", "Default": 6771093992792518656, "No": 6771093193802874880},
    {"day": 28, "Default": 6771094851312123904},
    {"day": 29, "question": 7, "answer": "Low vitamin B12", "Default": 6771095688235155456, "No": 6771095314115821568},
    {"day": 30, "Default": 6771096999482085376},
    {"day": 31, "question": 1, "answer": "PCOD", "Default": 6771097829434183680, "No": 6771097379286253568}
]}


def response_func(pa_ans, id):
    ret_value = ""
    for res in pa_ans["questions"]:
        print(res)
        if int(res["id"]) == int(id):
            ret_value = res
            break
    return ret_value


def get_answer(res, ans_id):
    ret_value = ""
    print(ans_id)
    for ans in res["fields"]:
        if int(ans["id"]) == int(ans_id):
            ret_value = ans
            break
    print("answer")
    print(ret_value)
    return ret_value["text"]


def send_message_response(pa_ans, day):
    rule = rules["days"][day - 1]

    if "question" in rule:
        question = rule["question"]
        question = response_func(pa_ans, question)
        if question["id"] == 1:
            answer = get_answer(question, question["answer"])
        if question["id"] == 2:
            answer = get_answer(question, question["answer"][1])

        if rule["answer"].lower() == answer.lower():
            ret_value = rule["Default"]
        else:
            ret_value = rule["No"]
    else:
        ret_value = rule["Default"]

    return ret_value


def freemium_journey():

    freemium_journey_active = int(Variable.get("freemium_journey_run", '0'))
    if freemium_journey_active == 1:
        return

    engine = get_data_from_db(db_type="mysql", conn_id="mysql_monolith")
    connection = engine.get_conn()
    cursor = connection.cursor()
    sql_query = str(Variable.get("freemium_journeysql_query", "select id, DATEDIFF(NOW(), created_at) from "
                                                              "zylaapi.patient_profile where created_at <= "
                                                              "NOW() - INTERVAL 1 DAY AND created_at "
                                                              ">= NOW() - INTERVAL 31 DAY and status = 11"))
    cursor.execute(sql_query)
    patient_id_dict = {}
    for row in cursor.fetchall():
        patient_id_dict[row[0]] = row[1]

    for key, value in patient_id_dict.items():

        try:
            pa_ans = get_pa_details(key)
            message = send_message_response(pa_ans, value)
            log.info("patient ID    " + key + "    day    " + value + "   message   " + message)
            user_id_list = []
            user_id_list.append(key)
            #process_custom_message(user_id_list, value)
        except Exception as e:
            log.error(e)
