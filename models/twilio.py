from twilio.rest import Client
import json


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

    def send_message(self, message):
        print(message)
        # self.channel.messages.create()
