import json


class PyJSON(object):
    def __init__(self, d: object) -> object:
        if type(d) is str:
            d = json.loads(d)
        self.from_dict(d)

    def from_dict(self, d) -> None:
        """
        Recursively generates object from dict d
        :param d: json to generate object from
        :type d: dict
        :return: None
        """
        self.__dict__ = {}
        for key, value in d.items():
            if type(value) is dict:
                value = PyJSON(value)
            self.__dict__[key] = value

    def to_dict(self) -> dict:
        """
        Convert :class:`PyJSON` to python dictionary
        :return: d
        :rtype: dict
        """
        d = {}
        for key, value in self.__dict__.items():
            if type(value) is PyJSON:
                value = value.to_dict()
            d[key] = value
        return d

    def __repr__(self) -> str:
        """
        :return: String representation of object
        :rtype: str
        """
        return str(self.to_dict())

    def __setitem__(self, key, value):
        self.__dict__[key] = value

    def __getitem__(self, key):
        return self.__dict__[key]
