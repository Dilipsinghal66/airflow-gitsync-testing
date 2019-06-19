from http import HTTPStatus

from common.functions import PyJSON
from common.helpers import get_user_by_filter, update_user_activity
import logging
from json import dumps


def sendStateMachineMessage(callback_data: str):
    callback_message = PyJSON(callback_data)
    callback_message.Attributes = PyJSON(callback_message.Attributes)
    channel_sid = callback_message.ChannelSid
    attributes = callback_message.Attributes
    state_info = {}
    state_obj = attributes.state
    current_state = state_obj.currentState
    current_action = state_obj.currentAction
    phone_number = callback_message.ClientIdentity
    user_filter= {
        "phoneNo": int(phone_number)
    }
    phone_data = get_user_by_filter(user_filter=user_filter, single=True)

    if phone_data:  # noqa E125
        if current_state == "chatbox":
            isCm = phone_data.get("isCm")
            if isCm:
                channel_filter = {
                    "chatInformation.providerData.channelSid": channel_sid
                }
                user_data = get_user_by_filter(user_filter=channel_filter, single=True)
                user_id = user_data.get("userId")
                if not user_id:
                    return True
                from common.helpers import send_chat_notification
                try:
                    data = attributes.to_dict()
                    data["message"]["index"] = callback_message.Index
                    send_chat_notification(userId=user_id, data=data, message=attributes.message.content.en)
                except Exception as e:
                    print(str(e))
            else:
                endpoint = "activity/" + str(phone_data.get("_id"))
                payload = {
                    "lastActivity": True
                }
                update_user_activity(endpoint=endpoint, payload=payload)
            return True
    user_id = phone_data.get("userId")
    try:
        from common.helpers import update_patient_status_on_sm
        update_patient_status_on_sm(user_id=user_id, sm_action=current_action)
    except Exception as e:
        print(e)

    # try:
    #     if current_action in list(PATIENT_STATUS_SM_MAP.keys()):
    #         update_patient_status_on_sm(user_id=user_id,
    #                                     sm_action=current_action)
    # except Exception as e:
    #     logger.info("status update failed.")
    # if current_action == "chatbox" and current_state == "chatbox":
    #     return True, 200
    # allowed_actions = []
    # for i in TRANSITIONS:
    #     if current_state == i[1]:
    #         allowed_actions.append(i[0])
    # if current_state == "chatbox" and not phone_data.isCm:  # noqa E125
    #     logger.info("update last seen of the user")
    #     return True, 200
    # if not current_action and current_state == "new":
    #     current_action = "onboard"
    # if not state_obj:
    #     message = "State object not found."
    #     logger.error(message)
    #     raise UnprocessableEntity(description=message)
    # if not current_state:
    #     message = "Current state value doesn't exist."
    #     logger.error(message)
    #     raise UnprocessableEntity(description=message)
    # if not current_action:
    #     message = "Current action value doesn't exist. "
    #     logger.error("message")
    #     raise UnprocessableEntity(description=message)
    # if current_action not in allowed_actions:
    #     message = "Current action: " + current_action + \
    #               " is not allowed for the state mentioned " + \
    #               current_state
    #     logger.error(message)
    #     raise UnprocessableEntity(description=message)
    # if current_action == PA_ACTION:
    #     level_url = HEALTH_PLAN_URL
    #     patient_data = {"patientId": patient_id}
    #     try:
    #         response = requests.post(level_url, json=patient_data)
    #
    #         if response.status_code == HTTPStatus.CREATED:  # noqa E125
    #             logger.info(
    #                 "health plan for patient " + str(
    #                     patient_id) + " created")
    #             user_payload = {
    #                 "userFlags.hideHealthPlan": False
    #             }
    #             logger.info(
    #                 "updating hideHealthPlan for patient to false for "
    #                 "patient " + str(patient_id))
    #             status_code = None
    #             try:
    #                 flag_update_response, status_code = \
    #                     user_model.update_user_details(
    #                         user_object=user,
    #                         payload=user_payload)
    #             except Exception as e:
    #                 try:
    #                     logger.warn(e)
    #                 except Exception as e:
    #                     pass
    #                 logger.warn("Failed in setting hideHealthPlan to "
    #                             "false for patient" + str(patient_id))
    #             if not status_code or (status_code != HTTPStatus.OK):
    #                 logger.warn("Failed in setting hideHealthPlan to "
    #                             "false for patient" + str(patient_id))
    #             logger.debug(flag_update_response)
    #         else:
    #             logger.info(
    #                 "health plan for patient " + str(
    #                     patient_id) + " unsuccessful")
    #     except Exception as e:
    #         logger.exception("Health Plan creation failed", exc_info=True)
    #     try:
    #         pa_progress_url = PROGRESS_URL + "/pa/" + str(patient_id)
    #         progress_response = requests.get(pa_progress_url)
    #         logger.debug("Progess response " + str(
    #             progress_response.status_code) + " with body "
    #                                              "" +
    #                      progress_response.text)
    #     except Exception as e:
    #         logger.exception("PA progress failed")
    #
    # machine.set_state(current_state)
    # possible_actions = {}
    # try:
    #     logger.debug("get patient flags for userid " + user_id)
    #     hide_health_plan = get_patient_flags(userId=user_id)
    # except Exception as e:
    #     logger.warn("Couldnt get health plan flag value")
    # machine.trigger(current_action)
    # state = machine.state
    # logger.info("Current state: " + state)
    # for i in TRANSITIONS:
    #     if i[1] != state:
    #         continue
    #     if not len(possible_actions):
    #         possible_actions = {}
    #     possible_actions.update({i[0]: ACTION_MESSAGES[i[0]]})
    #
    # message_type = ACTION_MESSAGES.get(current_action).get("type")
    # payload_data = {
    #     "action": current_action
    # }
    # response_data = post_resource_data(
    #     STATE_MACHINE_URL, data=payload_data)
    # data = response_data.text
    # data = json.loads(data)
    # logger.debug("State machine data:" + response_data.text)
    # if response_data.status_code != HTTPStatus.OK:
    #     return data, response_data.status_code
    # message_config = data.get("message_config")
    # config_values = {"lockTextBox": lock_text_box}
    # config_values["hideHealthPlan"] = hide_health_plan
    #
    # for k in message_config:
    #     config_values.update({k: True})
    # state_info["current_state"] = data.get("current_state")
    # state_info["previous_state"] = data.get("previous_state")
    # state_info["current_action"] = current_action
    # state_info["possible_actions"] = possible_actions
    # state_info["ui_config"] = config_values
    # state_info["message_type"] = message_type
    # message = data.get("state_message")
    # if message:
    #     chatservice.send_message(
    #         message=message,
    #         channel_sid=channel_sid,
    #         state_info=state_info, patient_status=patient_status,
    #         user_id=user_id)
    # auto_transition = data.get("auto_transition")
    # while auto_transition:
    #     logger.info(
    #         "Starting auto transition with delay of " +
    #         str(AUTO_TRANSITION_DELAY))
    #     sleep(AUTO_TRANSITION_DELAY)
    #     current_action = auto_transition
    #     if response_data.status_code != HTTPStatus.OK:
    #         return data, response_data.status_code
    #     message_config = data.get("message_config")
    #     config_values = {"lockTextBox": lock_text_box}
    #     for k in message_config:
    #         config_values.update({k: True})
    #     possible_actions = {}
    #     machine.trigger(current_action)
    #     state = machine.state
    #     for i in TRANSITIONS:
    #         if i[1] != state:
    #             continue
    #         if not len(possible_actions):
    #             possible_actions = {}
    #         possible_actions.update({i[0]: ACTION_MESSAGES[i[0]]})
    #     payload_data = {
    #         "action": auto_transition
    #     }
    #     response_data = post_resource_data(
    #         STATE_MACHINE_URL, data=payload_data)
    #     data = response_data.text
    #     data = json.loads(data)
    #     auto_transition = data.get("auto_transition")
    #     state = data.get("current_state")
    #     state_info["current_state"] = data.get("current_state")
    #     state_info["previous_state"] = data.get("previous_state")
    #     state_info["current_action"] = current_action
    #     state_info["possible_actions"] = possible_actions
    #     state_info["ui_config"] = config_values
    #     state_info["message_type"] = data.get("message_type")
    #     message = data.get("state_message")
    #     logger.info("Auto transition message " + message)
    #     if message:
    #         chatservice.send_message(
    #             message=message,
    #             channel_sid=channel_sid,
    #             state_info=state_info, patient_status=patient_status,
    #             user_id=user_id)
    #     logger.info("Auto transition message sent")
    # return True, 200
    # pass
