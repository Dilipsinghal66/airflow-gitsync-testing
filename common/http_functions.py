import json

from airflow.hooks.http_hook import HttpHook

from config import payload_required_methods, extra_http_options, \
    disable_response_check_methods

chat_headers = {
    "client": "Service",
    "auth_token": "0A44F17766A76D240552A6B3A12CAA38F37889B72E868AAE729A07C557B4B9CB"
}

def make_http_request(conn_id=None, method=None, payload=None, endpoint=""):
    if not conn_id:
        return False
    if not method:
        return False
    hook_obj = HttpHook(method=method, http_conn_id=conn_id)
    if method in payload_required_methods:
        payload = json.dumps(payload)
        response = hook_obj.run(endpoint=endpoint, data=payload, headers=chat_headers,
                                extra_options=extra_http_options)
    elif method in disable_response_check_methods:
        response = hook_obj.run(endpoint=endpoint)
        return response
    else:
        response = hook_obj.run(endpoint=endpoint,
                                extra_options=extra_http_options)
    if response:
        return response.status_code, response.json()
