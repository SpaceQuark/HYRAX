"""Job execution for hyrax-bask: makes what runs on this node match what the control plane asked for.

Every attempt is keyed by (job id, attempt number). The attempt number is a fencing token: this node
remembers the highest one it has seen for each job and refuses anything older. What the agent started is
written to a state file before the process exists, so a restarted agent picks its processes up again.
"""

import json
import logging
import os
import re
import shutil
import signal
import subprocess
import sys
import threading
import time
import urllib.parse
from pathlib import Path

from . import snapshot
from .client import ApiError

log = logging.getLogger("hyrax-bask")

FINAL = ("SUCCEEDED", "FAILED", "CANCELLED", "REJECTED")
CHUNK = 256 * 1024


def start_ticks(pid: int) -> int | None:
    """When the process started, in clock ticks since boot. With the pid, this identifies one process for good."""
    try:
        with open(f"/proc/{pid}/stat") as f:
            fields = f.read().rsplit(")", 1)[1].split()
    except (OSError, IndexError):
        return None
    return None if fields[0] == "Z" else int(fields[19])


def find_uv() -> str | None:
    """uv is not on a systemd service's PATH, so the usual install locations are tried as well."""
    candidates = [shutil.which("uv"), Path.home() / ".local/bin/uv", Path.home() / ".cargo/bin/uv",
                  Path.home() / ".local/share/hyrax/uv"]
    return next((str(path) for path in candidates if path and os.access(path, os.X_OK)), None)


class PrepareError(Exception):
    pass


class Executor:
    def __init__(self, home: Path, client, grace: float = 10.0):
        self.client = client
        self.grace = grace
        self.jobs_dir = home / "jobs"
        self.state_path = home / "state.json"
        self.attempts: dict[str, dict] = {}
        self.generations: dict[str, int] = {}
        self.children: dict[str, subprocess.Popen] = {}
        # Log upload progress. Kept in memory: the control plane says where to resume.
        self.uploaded: dict[str, int] = {}
        self.log_done: set[str] = set()
        # Output upload runs in threads so that a large checkpoint never delays a heartbeat. The threads
        # only add to these two; the main loop copies the result into the state file.
        self.outputs_done: set[str] = set()
        self.output_notes: dict[str, list[str]] = {}
        self.uploading: set[str] = set()
        self.jobs_dir.mkdir(parents=True, exist_ok=True)
        self._load()

    # ---- state file ----

    def _load(self) -> None:
        try:
            state = json.loads(self.state_path.read_text())
        except FileNotFoundError:
            return
        self.attempts = state["attempts"]
        self.generations = state["generations"]
        for key, record in self.attempts.items():
            if record["state"] != "STARTING":
                continue
            # The agent died between recording the attempt and recording its pid.
            try:
                pid = int((Path(record["dir"]) / "runner.pid").read_text())
            except (OSError, ValueError):
                record.update(state="FAILED", reason="the agent stopped while starting this attempt")
            else:
                record.update(state="RUNNING", pid=pid, ticks=start_ticks(pid))
        running = [key for key, record in self.attempts.items() if record["state"] == "RUNNING"]
        if running:
            log.info("picked up running attempts after restart: %s", ", ".join(running))
        self._save()

    def _save(self) -> None:
        temporary = self.state_path.with_suffix(".tmp")
        temporary.write_text(json.dumps({"attempts": self.attempts, "generations": self.generations}, indent=1))
        os.replace(temporary, self.state_path)

    # ---- one tick ----

    def report(self) -> list[dict]:
        """Attempts for the heartbeat. A finished attempt stays RUNNING until its log and its output files
        are uploaded, so the control plane never shows a job as done with half of what it produced."""
        reported = []
        for key, record in self.attempts.items():
            state = record["state"]
            if state in FINAL and record.get("dir") and (key not in self.log_done or not record.get("outputsDone")):
                state = "RUNNING"
            reported.append({
                "jobId": record["jobId"], "attempt": record["attempt"], "state": state, "pid": record.get("pid"),
                "exitCode": record.get("exitCode") if state in FINAL else None,
                "reason": record.get("reason") if state in FINAL else None,
            })
        return reported

    def apply(self, desired: dict, gpus: list[dict]) -> None:
        """Acts on a heartbeat reply. An attempt missing from `run` is left alone; only `cancel` stops one."""
        for ref in desired.get("forget", []):
            self._forget(ref)
        for ref in desired.get("cancel", []):
            self._cancel(ref)
        for run in desired.get("run", []):
            self._start(run, gpus)

    def poll(self) -> None:
        """Notices attempts that have ended, and escalates cancels that SIGTERM did not finish."""
        changed = False
        for key, record in self.attempts.items():
            if record["state"] in FINAL and record.get("dir") and not record.get("outputsDone"):
                changed |= self._collect_outputs(key, record)
            if record["state"] != "RUNNING":
                continue
            if self._alive(key, record):
                if record.get("termAt") and not record.get("killed") and time.time() - record["termAt"] > self.grace:
                    log.warning("attempt %s ignored SIGTERM for %.0fs, sending SIGKILL", key, self.grace)
                    self._signal(record, signal.SIGKILL)
                    record["killed"] = True
                    changed = True
                continue
            self.children.pop(key, None)
            try:
                code = int((Path(record["dir"]) / "exit_code").read_text())
            except (OSError, ValueError):
                code = None
            if record.get("termAt"):
                # The runner is gone; anything of the group that outlived it goes too.
                try:
                    os.killpg(record["pid"], signal.SIGKILL)
                except (ProcessLookupError, PermissionError):
                    pass
                record.update(state="CANCELLED", exitCode=code)
            elif code is None:
                record.update(state="FAILED", reason="the process vanished without an exit status (killed, or the machine rebooted)")
            elif (Path(record["dir"]) / "failed_stage").exists():
                record.update(state="FAILED", exitCode=code,
                              reason=(Path(record["dir"]) / "failed_stage").read_text() + " failed; see the log")
            else:
                record.update(state="SUCCEEDED" if code == 0 else "FAILED", exitCode=code)
            log.info("attempt %s ended: %s (exit %s)", key, record["state"], code)
            changed = True
        if changed:
            self._save()

    def _collect_outputs(self, key: str, record: dict) -> bool:
        """Starts the upload of a finished attempt's output files, or records that it has completed."""
        if key in self.outputs_done:
            notes = self.output_notes.pop(key, [])
            if notes:
                problem = f"{len(notes)} output file(s) were not stored: " + "; ".join(notes[:3])
                record["reason"] = f"{record['reason']}; {problem}" if record.get("reason") else problem
            record["outputsDone"] = True
            self.outputs_done.discard(key)
            self.uploading.discard(key)
            return True
        if key not in self.uploading:
            self.uploading.add(key)
            threading.Thread(target=self._upload_outputs, args=(key, dict(record)), daemon=True).start()
        return False

    def _upload_outputs(self, key: str, record: dict) -> None:
        outputs = Path(record["dir"]) / "outputs"
        base = f"/api/agent/jobs/{record['jobId']}/attempts/{record['attempt']}/artifacts"
        notes = []
        for path in sorted(outputs.rglob("*")) if outputs.is_dir() else []:
            # Regular files only. A symlink could point at anything this account can read.
            if path.is_symlink() or not path.is_file():
                continue
            relative = path.relative_to(outputs).as_posix()
            while True:
                try:
                    query = urllib.parse.urlencode({"path": relative, "sha256": snapshot.sha256_of(path)})
                    self.client.upload(f"{base}?{query}", path, timeout=600)
                except ApiError as error:
                    notes.append(f"{relative} ({error})")
                except FileNotFoundError:
                    pass
                except OSError as error:
                    # The control plane is away. The outputs wait here until it is back.
                    log.warning("could not upload %s of %s, retrying: %s", relative, key, error)
                    time.sleep(5)
                    continue
                break
        if notes:
            log.warning("outputs of %s not stored: %s", key, "; ".join(notes))
        self.output_notes[key] = notes
        self.outputs_done.add(key)

    def upload_logs(self) -> None:
        for key, record in self.attempts.items():
            if not record.get("dir") or key in self.log_done:
                continue
            path = f"/api/agent/jobs/{record['jobId']}/attempts/{record['attempt']}/logs"
            try:
                if key not in self.uploaded:
                    # An empty chunk asks where the stored log ends.
                    self.uploaded[key] = self._post_log(path, 0, b"")
                for _ in range(4):
                    try:
                        with open(Path(record["dir"]) / "output.log", "rb") as f:
                            f.seek(self.uploaded[key])
                            data = f.read(CHUNK)
                    except FileNotFoundError:
                        data = b""
                    if not data:
                        if record["state"] in FINAL:
                            self.log_done.add(key)
                        break
                    self.uploaded[key] = self._post_log(path, self.uploaded[key], data)
            except ApiError as error:
                # The log is full, or the control plane does not know the attempt: nothing more to send.
                log.warning("stopped uploading the log of %s: %s", key, error)
                self.log_done.add(key)
            except (OSError, ValueError):
                return

    def _post_log(self, path: str, offset: int, data: bytes) -> int:
        reply, _ = self.client.request("POST", f"{path}?offset={offset}", data, "application/octet-stream")
        return json.loads(reply)["offset"]

    # ---- transitions ----

    def _start(self, run: dict, gpus: list[dict]) -> None:
        job, attempt = run["jobId"], run["attempt"]
        key = f"{job}:{attempt}"
        if key in self.attempts:
            return
        if attempt <= self.generations.get(str(job), 0):
            self._refuse(key, job, attempt,
                         f"stale attempt: this node has already seen attempt {self.generations[str(job)]}")
            return
        older = [k for k, r in self.attempts.items() if r["jobId"] == job and r["state"] in ("STARTING", "RUNNING")]
        if older:
            # Never two attempts of one job at once. The newer one starts when the older has gone.
            for stale in older:
                self._cancel({"jobId": job, "attempt": self.attempts[stale]["attempt"]})
            return
        for uuid in run["gpus"]:
            gpu = next((g for g in gpus if g["uuid"] == uuid), None)
            if gpu is None:
                self._refuse(key, job, attempt, f"GPU {uuid} is not on this node")
                return
            held = sum(p["memoryMb"] or 0 for p in gpu["processes"] if p["type"] == "compute")
            if held > run["freeMaxComputeMb"]:
                self._refuse(key, job, attempt, f"GPU {gpu['index']} was taken in the meantime: other processes hold {held} MiB")
                return

        directory = self.jobs_dir / f"{job}-{attempt}"
        shutil.rmtree(directory, ignore_errors=True)
        (directory / "outputs").mkdir(parents=True)
        record = {"jobId": job, "attempt": attempt, "state": "STARTING", "dir": str(directory), "gpus": run["gpus"]}
        self.attempts[key] = record
        self.generations[str(job)] = attempt
        self._save()
        try:
            spec = self._prepare(run, directory)
        except (PrepareError, snapshot.SnapshotError, ApiError, OSError) as error:
            # Not REJECTED: that would requeue the job, and it would fail the same way everywhere.
            log.warning("could not prepare attempt %s: %s", key, error)
            (directory / "output.log").write_text(f"hyrax: {error}\n")
            record.update(state="FAILED", reason=f"could not prepare the job: {error}")
            self._save()
            return
        (directory / "spec.json").write_text(json.dumps(spec))
        # The agent's credential stays with the agent.
        env = {k: v for k, v in os.environ.items() if k not in ("HYRAX_AGENT_TOKEN", "HYRAX_TOKEN")}
        env.update(run.get("env") or {})
        for name, path in (run.get("datasets") or {}).items():
            env["HYRAX_DATASET_" + re.sub(r"[^A-Za-z0-9]", "_", name).upper()] = path
        env.update(
            CUDA_VISIBLE_DEVICES=",".join(run["gpus"]),
            HYRAX_JOB_ID=str(job),
            HYRAX_ATTEMPT=str(attempt),
            # Whatever the job writes here is uploaded when it ends, and `hyrax fetch` brings it back.
            HYRAX_OUTPUT_DIR=str(directory / "outputs"),
            PYTHONUNBUFFERED="1",
        )
        try:
            child = subprocess.Popen(
                [sys.executable, "-m", "hyrax.runner", str(directory)],
                cwd=directory, env=env, start_new_session=True,  # the runner changes into the job's own cwd
                stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            )
        except OSError as error:
            record.update(state="FAILED", reason=f"could not start: {error}")
        else:
            self.children[key] = child
            record.update(state="RUNNING", pid=child.pid, ticks=start_ticks(child.pid))
            log.info("started attempt %s as pid %d on %s: %s", key, child.pid, run["gpus"], run["command"])
        self._save()

    def _prepare(self, run: dict, directory: Path) -> dict:
        """Fetches and unpacks the job's source, and works out where and how the command runs."""
        spec = {"command": run["command"], "cwd": str(directory), "setup": None}
        source = run.get("snapshot")
        if source:
            archive = directory / "snapshot.tar.gz"
            arrived = self.client.download(f"/api/agent/snapshots/{source['id']}", archive)
            if arrived != source["sha256"]:
                raise PrepareError("the source snapshot does not match its checksum")
            snapshot.extract(archive, directory / "src")
            archive.unlink()
            workdir = run.get("workdir") or "."
            cwd = directory / "src" if workdir == "." else snapshot.safe_join(directory / "src", workdir)
            if not cwd.is_dir():
                raise PrepareError(f"the snapshot has no directory {workdir}")
            spec["cwd"] = str(cwd)
        if run.get("environment") == "uv":
            uv = find_uv()
            if not uv:
                raise PrepareError("uv is not installed on this node")
            spec["setup"] = {"command": [uv, "sync", "--locked"], "venv": str(directory / "venv")}
        return spec

    def _refuse(self, key: str, job: int, attempt: int, reason: str) -> None:
        log.warning("refused attempt %s: %s", key, reason)
        self.attempts[key] = {"jobId": job, "attempt": attempt, "state": "REJECTED", "reason": reason}
        self._save()

    def _cancel(self, ref: dict) -> None:
        key = f"{ref['jobId']}:{ref['attempt']}"
        record = self.attempts.get(key)
        if record is None:
            # Cancelled before it got here. Remember it so that it is never started late.
            self.attempts[key] = {"jobId": ref["jobId"], "attempt": ref["attempt"], "state": "CANCELLED",
                                  "reason": "cancelled before it started"}
            self.generations[str(ref["jobId"])] = max(self.generations.get(str(ref["jobId"]), 0), ref["attempt"])
        elif record["state"] == "RUNNING" and not record.get("termAt"):
            log.info("cancelling attempt %s (pid %d)", key, record["pid"])
            record["termAt"] = time.time()
            self._signal(record, signal.SIGTERM)
        else:
            return
        self._save()

    def _forget(self, ref: dict) -> None:
        key = f"{ref['jobId']}:{ref['attempt']}"
        record = self.attempts.get(key)
        if record is None or record["state"] not in FINAL:
            return
        directory = record.get("dir")
        # Only ever delete a directory this agent made for this attempt.
        if directory and Path(directory).parent == self.jobs_dir:
            shutil.rmtree(directory, ignore_errors=True)
        del self.attempts[key]
        self.uploaded.pop(key, None)
        self.log_done.discard(key)
        self.uploading.discard(key)
        self._save()

    # ---- processes ----

    def _alive(self, key: str, record: dict) -> bool:
        child = self.children.get(key)
        if child is not None:
            return child.poll() is None
        # Not our child (adopted after a restart): the pid counts only if it is still the same process.
        return record.get("ticks") is not None and start_ticks(record["pid"]) == record["ticks"]

    def _signal(self, record: dict, signum: int) -> None:
        """Signals the attempt's process group, after checking the pid has not been reused by something else."""
        if start_ticks(record["pid"]) != record.get("ticks"):
            return
        try:
            os.killpg(record["pid"], signum)
        except (ProcessLookupError, PermissionError):
            pass
