import time
from logging import Logger

from slack_bolt import Ack, Respond

from scream_message import SNOOZE_HOURS


def _resolved(body: dict, outcome: str) -> dict:
    message = body["message"]
    kept = [block for block in message["blocks"] if block["type"] != "actions"]
    return {
        "replace_original": True,
        "text": f"{message['text']} {outcome}",
        "blocks": [*kept, {"type": "context", "elements": [{"type": "mrkdwn", "text": outcome}]}],
    }


def claim_callback(ack: Ack, body: dict, respond: Respond, logger: Logger):
    try:
        ack()
        alert_id = body["actions"][0]["value"]
        user = body["user"]["id"]
        logger.info(f"alert {alert_id} claimed by {user}")
        respond(**_resolved(body, f"Claimed by <@{user}>. Future alerts for this process go to their DMs."))
    except Exception:
        logger.exception("Error handling scream claim")


def snooze_callback(ack: Ack, body: dict, respond: Respond, logger: Logger):
    try:
        ack()
        alert_id = body["actions"][0]["value"]
        user = body["user"]["id"]
        until = int(time.time()) + SNOOZE_HOURS * 3600
        logger.info(f"alert {alert_id} snoozed by {user} until {until}")
        respond(**_resolved(body, f"Snoozed by <@{user}> until <!date^{until}^{{time}}|{SNOOZE_HOURS} hours from now>."))
    except Exception:
        logger.exception("Error handling scream snooze")
