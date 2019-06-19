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

    def send_message(self, message):
        print(message)
        # self.channel.messages.create()
