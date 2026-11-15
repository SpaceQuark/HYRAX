from slack_bolt import App

from scream_message import CLAIM_ACTION_ID, SNOOZE_ACTION_ID

from .sample_action import sample_action_callback
from .scream import claim_callback, snooze_callback


def register(app: App):
    app.action("sample_action_id")(sample_action_callback)
    app.action(CLAIM_ACTION_ID)(claim_callback)
    app.action(SNOOZE_ACTION_ID)(snooze_callback)
