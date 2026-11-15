"""Runs one job attempt and records its exit status. Started by hyrax-bask as `python -m hyrax.runner DIR`.

It leads its own session, so the attempt is one process group that outlives an agent restart, and the exit
status lands in a file the agent can read even though it is no longer the parent.
"""

import json
import os
import signal
import subprocess
import sys
from pathlib import Path


def main() -> None:
    directory = Path(sys.argv[1])
    spec = json.loads((directory / "spec.json").read_text())
    (directory / "runner.pid").write_text(str(os.getpid()))
    # Cancel signals the whole group. The runner must survive that to record how the command ended.
    # A handler, not SIG_IGN: an ignored signal would be inherited by the command.
    signal.signal(signal.SIGTERM, lambda *_: None)
    with open(directory / "output.log", "ab", buffering=0) as log:
        try:
            child = subprocess.Popen(spec["command"], stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT)
        except OSError as error:
            log.write(f"hyrax: cannot start {spec['command'][0]}: {error}\n".encode())
            code = 127
        else:
            code = child.wait()
    temporary = directory / "exit_code.tmp"
    temporary.write_text(str(code))
    os.replace(temporary, directory / "exit_code")


if __name__ == "__main__":
    main()
