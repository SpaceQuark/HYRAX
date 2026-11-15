from logging import Logger

from slack_bolt import Say


def is_direct_message(event: dict) -> bool:
    return event.get("channel_type") == "im"


def direct_message_callback(event: dict, say: Say, logger: Logger):
    try:
        say(f"HYRAX heard you, <@{event['user']}>. You said: {event.get('text', '')}")
    except Exception:
        logger.exception("Error replying to direct message")
