"""hyrax-bask: the node agent. Sends a heartbeat every few seconds, outbound only."""

import argparse
import json
import logging
import os
import socket
import time
import urllib.error
import urllib.request

import pynvml

from . import DEFAULT_SERVER, __version__, telemetry

log = logging.getLogger("hyrax-bask")


def post_heartbeat(server: str, token: str | None, body: dict, timeout: float) -> dict:
    request = urllib.request.Request(
        f"{server.rstrip('/')}/api/agent/heartbeat",
        data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    if token:
        request.add_header("Authorization", f"Bearer {token}")
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return json.load(response)


def main() -> None:
    parser = argparse.ArgumentParser(prog="hyrax-bask", description="HYRAX node agent")
    parser.add_argument("--server", default=os.environ.get("HYRAX_SERVER", DEFAULT_SERVER))
    parser.add_argument("--node", default=os.environ.get("HYRAX_NODE", socket.gethostname()))
    parser.add_argument("--interval", type=float, default=3.0, help="seconds between heartbeats")
    parser.add_argument("--once", action="store_true", help="print one sample as JSON and exit")
    args = parser.parse_args()
    token = os.environ.get("HYRAX_AGENT_TOKEN")

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    pynvml.nvmlInit()

    if args.once:
        print(json.dumps(telemetry.sample(), indent=2))
        return

    log.info("node %s reporting to %s every %.0fs", args.node, args.server, args.interval)
    connected = None
    while True:
        started = time.monotonic()
        body = {
            "node": args.node,
            "agentVersion": __version__,
            "gpus": telemetry.sample(),
            # Job attempts this agent knows about. Empty until milestone 3.
            "attempts": [],
        }
        try:
            # The response carries desired job state; nothing to act on until milestone 3.
            post_heartbeat(args.server, token, body, timeout=args.interval)
            if connected is not True:
                log.info("connected to control plane")
            connected = True
        except (urllib.error.URLError, OSError, ValueError) as error:
            # Log once per outage, then keep retrying quietly.
            if connected is not False:
                log.warning("control plane unreachable: %s", error)
            connected = False
        time.sleep(max(0.0, args.interval - (time.monotonic() - started)))


if __name__ == "__main__":
    main()
