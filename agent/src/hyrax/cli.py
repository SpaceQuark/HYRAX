"""hyrax: the CLI. Only `status` exists so far."""

import argparse
import json
import os
import sys
import urllib.error
import urllib.request

from . import DEFAULT_SERVER


def status(server: str) -> int:
    try:
        with urllib.request.urlopen(f"{server.rstrip('/')}/api/nodes", timeout=5) as response:
            nodes = json.load(response)
    except (urllib.error.URLError, OSError) as error:
        print(f"hyrax: cannot reach {server}: {error}", file=sys.stderr)
        return 1
    if not nodes:
        print("No nodes have reported yet.")
        return 0
    for node in nodes:
        print(f"{node['node']}  {'online' if node['online'] else 'OFFLINE'}")
        for gpu in node["gpus"]:
            utilization = "?" if gpu["utilization"] is None else gpu["utilization"]
            print(
                f"  [{gpu['index']}] {gpu['name']:<28} {utilization:>3}%  "
                f"{gpu['memoryUsedMb']:>6} / {gpu['memoryTotalMb']} MiB  "
                f"{gpu['temperatureC']}°C"
            )
            for proc in gpu["processes"]:
                memory = "?" if proc["memoryMb"] is None else proc["memoryMb"]
                command = (proc["command"] or "?")[:60]
                print(f"        {proc['pid']:>7} {proc['user'] or '?':<10} {memory:>6} MiB  {command}")
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(prog="hyrax")
    parser.add_argument("--server", default=os.environ.get("HYRAX_SERVER", DEFAULT_SERVER))
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("status", help="show every node, GPU and process")
    args = parser.parse_args()
    sys.exit(status(args.server))


if __name__ == "__main__":
    main()
