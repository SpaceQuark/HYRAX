import json
import sys
from dataclasses import dataclass

CLAIM_ACTION_ID = "scream_claim"
SNOOZE_ACTION_ID = "scream_snooze"
SNOOZE_HOURS = 2


@dataclass
class IdleProcess:
    alert_id: str
    node: str
    gpu_index: int
    gpu_name: str
    pid: int
    os_user: str
    command: str
    memory_gib: float
    idle_minutes: int


def _duration(minutes: int) -> str:
    hours, rest = divmod(minutes, 60)
    if hours and rest:
        return f"{hours} h {rest} min"
    return f"{hours} h" if hours else f"{rest} min"


def summary_blocks(process: IdleProcess) -> list[dict]:
    return [
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    f"*Idle GPU memory on {process.node}, GPU {process.gpu_index}*\n"
                    f"PID {process.pid} has held {process.memory_gib:.1f} GiB "
                    f"for {_duration(process.idle_minutes)} at 0% utilization.\n"
                    f"`{process.command}`"
                ),
            },
        },
        {
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": f"{process.gpu_name} · OS user `{process.os_user}` · HYRAX only pings, it never kills.",
                }
            ],
        },
    ]


def scream_message(process: IdleProcess, owner_known: bool) -> dict:
    buttons = []
    if not owner_known:
        buttons.append(
            {
                "type": "button",
                "action_id": CLAIM_ACTION_ID,
                "style": "primary",
                "text": {"type": "plain_text", "text": "This is mine"},
                "value": process.alert_id,
            }
        )
    buttons.append(
        {
            "type": "button",
            "action_id": SNOOZE_ACTION_ID,
            "text": {"type": "plain_text", "text": f"Snooze {SNOOZE_HOURS} hours"},
            "value": process.alert_id,
        }
    )
    return {
        "text": (
            f"Idle GPU memory on {process.node}, GPU {process.gpu_index}: PID {process.pid} has held "
            f"{process.memory_gib:.1f} GiB for {_duration(process.idle_minutes)}."
        ),
        "blocks": [
            *summary_blocks(process),
            {"type": "actions", "block_id": "scream_actions", "elements": buttons},
        ],
    }


SAMPLE = IdleProcess(
    alert_id="sample-1",
    node="node-3",
    gpu_index=0,
    gpu_name="RTX 4090",
    pid=48291,
    os_user="alice",
    command="python train.py --config configs/vit_large.yaml",
    memory_gib=22.0,
    idle_minutes=130,
)

if __name__ == "__main__":
    channel = sys.argv[1]
    owner_known = len(sys.argv) > 2 and sys.argv[2] == "owned"
    print(json.dumps({"channel": channel, **scream_message(SAMPLE, owner_known)}))
