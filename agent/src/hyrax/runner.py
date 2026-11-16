"""Runs one job attempt and records its exit status. Started by hyrax-bask as `python -m hyrax.runner DIR`.

It leads its own session, so the attempt is one process group that outlives an agent restart, and the exit
status lands in a file the agent can read even though it is no longer the parent.
"""

import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path


def main() -> None:
    directory = Path(sys.argv[1])
    spec = json.loads((directory / "spec.json").read_text())
    (directory / "runner.pid").write_text(str(os.getpid()))
    # Cancel signals the whole group. The runner must survive that to record how the command ended.
    # A handler, not SIG_IGN: an ignored signal would be inherited by the command.
    signal.signal(signal.SIGTERM, lambda *_: None)
    cwd = spec.get("cwd") or str(directory)
    env = dict(os.environ)
    with open(directory / "output.log", "ab", buffering=0) as log:
        code = prepare(spec.get("setup"), cwd, env, log)
        if code != 0:
            log.write(f"hyrax: environment setup failed (exit {code}); the command was not run\n".encode())
            (directory / "failed_stage").write_text("environment setup")
        else:
            try:
                child = subprocess.Popen(
                    spec["command"], cwd=cwd, env=env, stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT)
            except OSError as error:
                log.write(f"hyrax: cannot start {spec['command'][0]}: {error}\n".encode())
                code = 127
            else:
                code = child.wait()
    temporary = directory / "exit_code.tmp"
    temporary.write_text(str(code))
    os.replace(temporary, directory / "exit_code")


def prepare(setup: dict | None, cwd: str, env: dict, log) -> int:
    """Builds the job's own virtual environment from the project's lockfile and puts it first on PATH.

    `uv sync --locked` fails if uv.lock does not match pyproject.toml. It never re-resolves, so the job
    runs with exactly the versions that were locked on the submitting machine.
    """
    if not setup:
        return 0
    log.write(f"hyrax: creating the environment: {' '.join(setup['command'])}\n".encode())
    started = time.monotonic()
    try:
        code = subprocess.call(
            setup["command"], cwd=cwd, env={**env, "UV_PROJECT_ENVIRONMENT": setup["venv"]},
            stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT)
    except OSError as error:
        log.write(f"hyrax: cannot run {setup['command'][0]}: {error}\n".encode())
        return 127
    if code == 0:
        log.write(f"hyrax: environment ready in {time.monotonic() - started:.1f}s\n".encode())
        env["VIRTUAL_ENV"] = setup["venv"]
        env["PATH"] = os.path.join(setup["venv"], "bin") + os.pathsep + env.get("PATH", "")
    return code


if __name__ == "__main__":
    main()
