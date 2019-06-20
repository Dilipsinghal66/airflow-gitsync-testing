import json

from twilio.rest import Client


class ChatService():
    def __init__(self, account_sid, auth_token, service_sid, **kwargs):
        self.account_sid = account_sid
        self.auth_token = auth_token
        self.service_sid = service_sid
        self.set_service()

    def set_service(self):
        self.service = Client(username=self.account_sid, password=self.auth_token).chat.services.get(self.service_sid)

    def set_channel(self, channel_sid):
        self.channel = self.service.channels.get(sid=channel_sid)

    def get_sender(self):
        members = self.channel.members.list()
        cm_id = None
        for member in members:
            member_attr = json.loads(member.attributes)
            isCm = member_attr.get("isCm")
            if isCm:
                cm_id = member.identity
        return cm_id

    def send_message(self, attributes):
        from_ = self.get_sender()
        attributes = json.dumps(attributes)
        self.channel.messages.create(from_=from_, attributes=attributes)


def generate_twilio_message(**kwargs):
    from common.helpers import redis_conn_twilio_message
    message = kwargs.get("message")
    user_id = kwargs.get("user_id")
    state_info = kwargs.get("state_info")
    patient_status = kwargs.get("state_info")
    message = message.replace("\\n", "\n")
    channel_sid = kwargs.get("channel_sid")
    # logger.info("user id " + str(user_id) + " patient_status " + str(
    #     patient_status))
    lockVitals = True
    lockReporting = True
    lockTextBox = True
    ui_config = state_info.get("ui_config", None)
    hideHealthPlan = ui_config.get("hideHealthPlan", False)
    # if patient_status:
    #     _, notification, lockTextBox, lockReporting, lockVitals = \
    #         get_user_status(
    #             patient_status)
    #     if patient_status == 11:
    #         patient_level = get_patient_level(userId=user_id)
    #         if patient_level:
    #             if patient_level >= 2:
    #                 lockReporting = False
    #                 lockVitals = False
    #     logger.info("Lock text box " + str(lockTextBox))
    # if unlock_report:
    #     lockReporting = False
    # if unlock_vitals:
    #     lockVitals = False
    # if hide_health_plan:
    #     hideHealthPlan = hide_health_plan
    # if not notification:
    #     if not send_reminder:
    #         return True, 200

    twilio_message = {
        "state": {
            "currentState": state_info["current_state"],
            "currentAction": state_info["current_action"],
            "metadata": {
                "previousState": state_info[
                    "previous_state"],
                "isReversible": False,
                "uiConfig": {
                    "textBox": (True if state_info["ui_config"].get(
                        "textBox") else False) if state_info.get(
                        "ui_config") else False,
                    "menu": (
                        True if state_info["ui_config"].get(
                            "menu") else False) if state_info.get(
                        "ui_config") else False,
                    "lockTextBox": lockTextBox if
                    lockTextBox else False,
                    "showTour": (
                        True if state_info["ui_config"].get(
                            "showTour") else False) if
                    state_info.get(
                        "ui_config") else False,
                    "lockReporting": lockReporting if
                    lockReporting else False,
                    "lockVitals": lockVitals if lockVitals
                    else False,
                    "hideHealthPlan": hideHealthPlan
                },
                "possibleActions": state_info[
                    "possible_actions"]
            }
        },
        "message": {
            "type": state_info['message_type'],
            "metadata": {
                "id": state_info.get("metadata", None)
            },
            "content": {
                "en": message,
            },
            "url": "url if any in the message"
        }
    }
    redis_channel_key = channel_sid + "_send_twilio_message"
    #logger.debug("redis channel key" + redis_channel_key)
    redis_payload = json.dumps({
        "channelSid": channel_sid,
        "attributes": twilio_message
    })
    #logger.debug("redis data " + json.dumps(redis_payload))
    redis_conn_twilio_message.rpush(redis_channel_key, redis_payload)
    # twilio_message = channel.messages.create(
    #  from_=str(care_manager_id),
    #  attributes=json.dumps(twilio_message))
    # return twilio_message
