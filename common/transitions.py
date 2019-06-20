from transitions import Machine

from common.helpers import make_http_request, update_patient_status_on_sm

_, STATES = make_http_request(http_conn_id="http_statemachine_url", endpoint="transition/states", method="GET")

_, TRANSITIONS = make_http_request(http_conn_id="http_statemachine_url", endpoint="transition/data", method="GET")

_, TRIGGERS = make_http_request(http_conn_id="http_statemachine_url", endpoint="transition/trigger", method="GET")

_, ACTION_MESSAGES = make_http_request(http_conn_id="http_statemachine_url", endpoint="transition/trigger/messages",
                                       method="GET")

_, UI_CONFIG = make_http_request(http_conn_id="http_statemachine_url", endpoint="transition/states/ui_config",
                                 method="GET")


class StateTransition(object):
    def __init__(self, user_id, sm_action):
        self.user_id = user_id
        self.sm_action = sm_action

    def transition_callback(self):
        update_patient_status_on_sm(user_id=self.user_id, sm_action=self.sm_action)


machine = Machine(
    states=STATES,
    transitions=TRANSITIONS,
    initial="new",
    ignore_invalid_triggers=True,
    auto_transitions=False)
