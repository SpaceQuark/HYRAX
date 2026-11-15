"""hyrax-bask: the node agent. Reports telemetry and runs jobs. Outbound connections only."""

import argparse
import json
import logging
import os
import socket
import time
from pathlib import Path

import pynvml

from . import DEFAULT_SERVER, __version__, telemetry
from .client import ApiError, Client
from .executor import Executor

log = logging.getLogger("hyrax-bask")


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

    client = Client(args.server, token, os.environ.get("HYRAX_CA"))
    # Running commands for a server we cannot authenticate, or that cannot authenticate us, is not on offer.
    executor = None
    if client.secure and token:
        home = Path(os.environ.get("HYRAX_HOME", Path.home() / ".local" / "share" / "hyrax"))
        executor = Executor(home, client)
    else:
        log.warning("telemetry only: running jobs needs an https server and HYRAX_AGENT_TOKEN")

    log.info("node %s reporting to %s every %.0fs", args.node, args.server, args.interval)
    connected = None
    while True:
        started = time.monotonic()
        gpus = telemetry.sample()
        if executor:
            executor.poll()
            executor.upload_logs()
        body = {
            "node": args.node,
            "agentVersion": __version__,
            "gpus": gpus,
            "attempts": executor.report() if executor else [],
        }
        try:
            desired = client.json("POST", "/api/agent/heartbeat", body, timeout=args.interval)
            if connected is not True:
                log.info("connected to control plane")
            connected = True
        except (ApiError, OSError, ValueError) as error:
            # Log once per outage, then keep retrying quietly. Running jobs carry on regardless.
            if connected is not False:
                log.warning("control plane unreachable: %s", error)
            connected = False
        else:
            if executor:
                try:
                    executor.apply(desired or {}, gpus)
                except Exception:
                    # One bad command must not take telemetry down with it.
                    log.exception("could not apply the desired state")
        time.sleep(max(0.0, args.interval - (time.monotonic() - started)))


if __name__ == "__main__":
    main()
