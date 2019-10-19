import json
from random import choice

from airflow.hooks.http_hook import HttpHook
from twilio.rest import Client

active_cm_attributes = {
    "isCm": True,
    "activeCm": True
}


def get_twilio_service():
    """
    This function returns Twilio chat service instance.
    Credentials are fetched from http hook defined in airflow.

    - account_sid Account Sid for the given instance
    - auth_token Auth token for twilio authentication
    - service_sid Service to be fetched from twilio

    :return: TwilioChat instance
    """
    twilio_hook = HttpHook().get_connection(conn_id="http_twilio")
    account_sid = twilio_hook.login
    auth_token = twilio_hook.password
    service_sid = twilio_hook.extra_dejson.get("service_sid")
    twilio_conn = Client(account_sid, auth_token)
    service = twilio_conn.chat.services.get(sid=service_sid)
    return service


def get_twilio_user_details_from_db(user=None):
    provider_data = user.get("chatInformation", {}).get("providerData", {})
    channel_sid = provider_data.get("channelSid", None)
    if not channel_sid:
        raise ValueError("Channel sid not found for user.")
    identity = provider_data.get("identity", None)
    if not identity:
        raise ValueError("Identity not found for user. ")
    return channel_sid, identity


def if_exists_active_cm(user_channel=None, user_identity=None, service=None):
    active_cm_exists = False
    cm_member = None
    channel = service.channels(user_channel).fetch()
    members = channel.members.list()
    if len(members) < 1:
        raise ValueError("Something is wrong with channel " + user_channel)
    for member in members:
        if not int(member.identity) == int(user_identity):
            attributes = json.loads(member.attributes)
            active_cm = attributes.get("activeCm")
            if active_cm:
                active_cm_exists = True
            cm_member = member
    return active_cm_exists, cm_member, channel


def swap_cm_with_active(old_cm=None, channel=None):
    from common.helpers import active_cm_list
    active_cm = choice(active_cm_list)
    if old_cm:
        old_cm.delete()
    channel.members.create(
        active_cm,
        attributes=json.dumps(active_cm_attributes)
    )
    return active_cm


def process_switch(user=None, service=None):
    if not user:
        raise ValueError("User can not be null. ")
    if not service:
        raise ValueError("Service can not be null. ")
    user_channel, user_identity = get_twilio_user_details_from_db(
        user=user)
    has_active_cm, cm_member, channel = if_exists_active_cm(
        user_channel=user_channel,
        user_identity=user_identity,
        service=service)
    if has_active_cm:
        print(
            "Active cm " + str(cm_member.identity) + " is assigned to " + str(
                user_identity) + "in channel " + user_channel + ". Nothing to do.")
        return False
    print("Active cm is not assigned. Processing further. ")
    active_cm = swap_cm_with_active(old_cm=cm_member, channel=channel)
    return active_cm
