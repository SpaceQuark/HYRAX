import re

from slack_bolt import App

from .direct_message import direct_message_callback, is_direct_message
from .sample_message import sample_message_callback


# To receive messages from a channel or dm your app must be a member!
def register(app: App):
    app.event({"type": "message", "subtype": None}, matchers=[is_direct_message])(direct_message_callback)
    app.message(re.compile("(hi|hello|hey)"))(sample_message_callback)
