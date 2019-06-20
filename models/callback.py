import json


class TwilioMessageContent(object):
    def init_data(self, request_data: dict):
        classes = {}
        for k in request_data:
            if k in classes.keys():
                obj = classes.get(k)()
                obj.init_data(request_data.get(k))
                self.__setattr__(k, obj)
            else:
                self.__setattr__(k, request_data.get(k))
        pass


class TwilioMessage(object):

    def init_data(self, request_data: dict):
        classes = {"content": TwilioMessageContent}
        for k in request_data:
            if k in classes.keys():
                obj = classes.get(k)()
                obj.init_data(request_data.get(k))
                self.__setattr__(k, obj)
            else:
                self.__setattr__(k, request_data.get(k))
        pass

    @property
    def __dict__(self):
        return {"content": self.content.__dict__}


class MessageState(object):
    def __init__(self):
        self.currentState = None
        self.currentAction = None
        pass

    def init_data(self, request_data: dict):
        classes = {}
        for k in request_data:
            if k in classes.keys():
                obj = classes.get(k)()
                obj.init_data(request_data.get(k))
                self.__setattr__(k, obj)
            else:
                self.__setattr__(k, request_data.get(k))
        pass


class MessageAttributes(object):
    def __init__(self):
        self.message = None
        self.state = None
        self.url = None
        self.uuid = None

    def init_data(self, request_data: dict):
        classes = {"state": MessageState, "message": TwilioMessage}
        for k in request_data:
            if k in classes.keys():
                obj = classes.get(k)()
                obj.init_data(request_data.get(k))
                self.__setattr__(k, obj)
            else:
                self.__setattr__(k, request_data.get(k))

    @property
    def __dict__(self):
        return {"state": self.state.__dict__, "message": self.message.__dict__, "uuid": self.uuid}


class CallbackMessage(object):

    def __init__(self):
        self.ChannelSid = None
        self.ClientIdentity = None
        self.EventType = None
        self.InstanceSid = None
        self.retryCount = None
        self.Attributes: MessageAttributes = None
        self.DateCreated = None
        self.Index = None
        self.From = None
        self.MessageSid = None
        self.Body = None
        self.AccountSid = None
        self.duplicate = False

    def init_data(self, request_data: dict):
        classes = {"Attributes": MessageAttributes}
        for k in request_data:
            if k in classes.keys():
                obj = classes.get(k)()
                obj.init_data(json.loads(request_data.get(k)))
                self.__setattr__(k, obj)
            else:
                self.__setattr__(k, request_data.get(k))
