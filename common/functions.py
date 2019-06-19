import json
from types import SimpleNamespace


def get_python_object(data_str: str):
    obj_data = json.loads(data_str, object_hook=lambda d: SimpleNamespace(**d))
    return obj_data

