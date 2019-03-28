import json
from time import sleep
from random import randint

import requests
from airflow.contrib.hooks.mongo_hook import MongoHook
from airflow.hooks.http_hook import HttpHook
from airflow.models import Variable


def get_parsed_resource_data(resource_url: str):
  data = None
  try:
    response = requests.get(resource_url)
    data = response.text
    data = json.loads(data)
  except Exception as e:
    print(str(e))
  return data


def send_reminder(**kwargs):
  time_data_url = "https://services.zyla.in/statemachine/scheduler/19:00/messages/1"
  time_data = get_parsed_resource_data(time_data_url)
  messages = time_data.get("messages")[0]
  m_len = len(messages) - 1
  m_idx = randint(0, m_len)
  message = messages[m_idx]
  action = time_data.get("action")
  user_db = MongoHook(conn_id="mongo_user_db").get_conn().get_default_database()
  test_user_id = int(Variable.get("test_user_id", '0'))
  processing_batch_size = int(Variable.get("processing_batch_size", 100))
  payload = {
    "action": action,
    "message": message,
    "is_notification": False
  }
  user = user_db.get_collection("user")
  user_filter = {
    "userStatus": {"$in": [4]}
  }
  user_data = user.find(user_filter, {"userId": 1}).batch_size(100)
  http_hook = HttpHook(
    method="POST",
    http_conn_id="chat_service_url"
  )
  user_id_list = []
  while user_data.alive:
    for user in user_data:
      user_id = user.get("userId")
      if test_user_id and int(user_id) != test_user_id:
        continue
      user_id_list.append(user_id)

  for i in range(0, len(user_id_list) + 1, processing_batch_size):
    _id_list = user_id_list[i:i + processing_batch_size]
    sleep(1)
    for user_id in _id_list:
      try:
        http_hook.run(endpoint="/api/v1/chat/user/" + str(user_id) + "/message", data=json.dumps(payload))
      except Exception as e:
        raise ValueError(str(e))
