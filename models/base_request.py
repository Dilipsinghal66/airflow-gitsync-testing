from urllib3.poolmanager import PoolManager


class BaseRequestModel(PoolManager):

    ## Standard requests.
    def get(self):
        raise NotImplementedError()

    def post(self):
        raise NotImplementedError()

    def patch(self):
        raise NotImplementedError()

    def put(self):
        raise NotImplementedError()


    ## parsed requests
    def get_parsed_data(self):
        raise NotImplementedError()

