"""The executor against real processes and a stand-in control plane."""

import hashlib
import json
import shutil
import sys
import time
from pathlib import Path

import pytest

from hyrax import snapshot
from hyrax.client import ApiError
from hyrax.executor import Executor

GPU = "GPU-test-0"


class FakeServer:
    """What the executor needs from the control plane: log upload, output upload, snapshot download."""

    def __init__(self):
        self.logs: dict[str, bytes] = {}
        self.outputs: dict[str, bytes] = {}
        self.snapshots: dict[int, Path] = {}
        self.down = False

    def request(self, method, path, body=None, content_type=None, timeout=None):
        if self.down:
            raise OSError("control plane is down")
        key, offset = path.split("?offset=")
        stored = self.logs.setdefault(key, b"")
        if int(offset) == len(stored):
            self.logs[key] = stored + (body or b"")
        return json.dumps({"offset": len(self.logs[key])}).encode(), {}

    def upload(self, path, file_path, timeout=None):
        if self.down:
            raise OSError("control plane is down")
        query = dict(part.split("=") for part in path.split("?")[1].split("&"))
        data = Path(file_path).read_bytes()
        assert hashlib.sha256(data).hexdigest() == query["sha256"]
        if "refused" in query["path"]:
            raise ApiError(413, "413 too large")
        self.outputs[query["path"].replace("%2F", "/")] = data
        return {"bytes": len(data)}

    def download(self, path, file_path, timeout=None):
        source = self.snapshots[int(path.rsplit("/", 1)[1])]
        shutil.copy(source, file_path)
        return snapshot.sha256_of(Path(file_path))

    def log(self, job, attempt=1):
        return self.logs.get(f"/api/agent/jobs/{job}/attempts/{attempt}/logs", b"").decode()


def gpus(held_mb=0):
    processes = [{"pid": 1, "type": "compute", "memoryMb": held_mb}] if held_mb else []
    return [{"index": 0, "uuid": GPU, "processes": processes}]


def run(job, command, attempt=1, **extra):
    return {"jobId": job, "attempt": attempt, "command": command, "env": {"GREETING": "hello"}, "gpus": [GPU],
            "freeMaxComputeMb": 500, **extra}


def python(code):
    return [sys.executable, "-c", code]


@pytest.fixture
def server():
    return FakeServer()


@pytest.fixture
def executor(tmp_path, server):
    return Executor(tmp_path / "home", server, grace=1.0)


def settle(executor, job, attempt=1, timeout=15.0):
    """Ticks the executor like the agent loop does until the attempt is reported finished."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        executor.poll()
        executor.upload_logs()
        for report in executor.report():
            if (report["jobId"], report["attempt"]) == (job, attempt) and report["state"] != "RUNNING":
                return report
        time.sleep(0.05)
    raise AssertionError(f"attempt {job}:{attempt} did not finish: {executor.report()}")


def test_runs_a_command_with_its_gpu_and_uploads_the_whole_log_before_reporting(executor, server):
    executor.apply({"run": [run(1, python("import os; print(os.environ['GREETING'], os.environ['CUDA_VISIBLE_DEVICES'], "
                                          "os.environ['HYRAX_JOB_ID'], 'HYRAX_AGENT_TOKEN' in os.environ)"))]}, gpus())
    assert executor.report()[0]["state"] == "RUNNING"

    report = settle(executor, 1)

    assert (report["state"], report["exitCode"]) == ("SUCCEEDED", 0)
    assert server.log(1) == f"hello {GPU} 1 False\n"


def test_a_failing_command_reports_its_exit_code(executor):
    executor.apply({"run": [run(1, python("raise SystemExit(7)"))]}, gpus())
    report = settle(executor, 1)
    assert (report["state"], report["exitCode"]) == ("FAILED", 7)


def test_a_command_that_does_not_exist_fails_with_a_message_in_the_log(executor, server):
    executor.apply({"run": [run(1, ["/no/such/program"])]}, gpus())
    report = settle(executor, 1)
    assert (report["state"], report["exitCode"]) == ("FAILED", 127)
    assert "cannot start /no/such/program" in server.log(1)


def test_starting_the_same_attempt_twice_starts_one_process(executor):
    desired = {"run": [run(1, python("import time; time.sleep(30)"))]}
    executor.apply(desired, gpus())
    pid = executor.report()[0]["pid"]
    executor.apply(desired, gpus())
    assert [r["pid"] for r in executor.report()] == [pid]
    executor.apply({"cancel": [{"jobId": 1, "attempt": 1}]}, gpus())
    settle(executor, 1)


def test_an_attempt_missing_from_the_reply_keeps_running_and_only_cancel_stops_it(executor):
    executor.apply({"run": [run(1, python("import time; time.sleep(30)"))]}, gpus())
    executor.apply({}, gpus())
    executor.poll()
    assert executor.report()[0]["state"] == "RUNNING"

    time.sleep(0.5)  # let the command start, so that the exit status is the command's
    executor.apply({"cancel": [{"jobId": 1, "attempt": 1}]}, gpus())
    report = settle(executor, 1)
    assert (report["state"], report["exitCode"]) == ("CANCELLED", -15)


def test_cancel_escalates_to_sigkill_and_takes_the_whole_process_group(executor, tmp_path):
    marker = tmp_path / "child.pid"
    stubborn = ("import os, signal, subprocess, sys, time; signal.signal(signal.SIGTERM, signal.SIG_IGN); "
                f"child = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(60)']); "
                f"open({str(marker)!r}, 'w').write(str(child.pid)); time.sleep(60)")
    executor.apply({"run": [run(1, python(stubborn))]}, gpus())
    while not marker.exists() or not marker.read_text():
        time.sleep(0.05)

    executor.apply({"cancel": [{"jobId": 1, "attempt": 1}]}, gpus())
    report = settle(executor, 1)

    assert report["state"] == "CANCELLED"
    time.sleep(0.2)
    assert not Path(f"/proc/{marker.read_text()}").exists() or "Z" in Path(f"/proc/{marker.read_text()}/stat").read_text()


def test_a_gpu_taken_by_hand_is_rejected_not_started(executor):
    executor.apply({"run": [run(1, python("print('should not run')"))]}, gpus(held_mb=9000))
    report = executor.report()[0]
    assert report["state"] == "REJECTED"
    assert "9000 MiB" in report["reason"]
    assert not (executor.jobs_dir / "1-1").exists()


def test_an_older_attempt_of_a_job_is_refused_once_a_newer_one_was_seen(executor):
    executor.apply({"run": [run(1, python("pass"), attempt=2)]}, gpus())
    settle(executor, 1, attempt=2)
    executor.apply({"run": [run(1, python("print('stale')"), attempt=1)]}, gpus())
    stale = next(r for r in executor.report() if r["attempt"] == 1)
    assert stale["state"] == "REJECTED" and "stale" in stale["reason"]


def test_a_cancel_that_arrives_first_means_the_attempt_never_starts(executor):
    executor.apply({"cancel": [{"jobId": 1, "attempt": 1}]}, gpus())
    assert executor.report()[0]["state"] == "CANCELLED"
    executor.apply({"run": [run(1, python("print('late')"))]}, gpus())
    assert [r["state"] for r in executor.report()] == ["CANCELLED"]


def test_a_restarted_agent_picks_up_its_running_process_and_learns_the_exit_code(tmp_path, server):
    first = Executor(tmp_path / "home", server)
    first.apply({"run": [run(1, python("import time; time.sleep(1.5); print('still here'); raise SystemExit(3)"))]}, gpus())
    pid = first.report()[0]["pid"]

    # A new agent process: same state file, but it is not the parent and holds no handle on the child.
    second = Executor(tmp_path / "home", server)
    assert second.children == {}
    assert second.report() == [{"jobId": 1, "attempt": 1, "state": "RUNNING", "pid": pid, "exitCode": None, "reason": None}]
    first.children[next(iter(first.children))].wait()  # reap, as init would for an orphan
    report = settle(second, 1)

    assert (report["state"], report["exitCode"]) == ("FAILED", 3)
    assert server.log(1) == "still here\n"


def test_a_process_that_vanished_without_an_exit_status_fails_and_a_reused_pid_is_not_mistaken_for_it(tmp_path, server):
    first = Executor(tmp_path / "home", server)
    first.apply({"run": [run(1, python("import time; time.sleep(30)"))]}, gpus())
    state = json.loads(first.state_path.read_text())
    # As after a reboot: the pid now belongs to some other process (this one), with another start time.
    import os
    state["attempts"]["1:1"]["pid"] = os.getpid()
    first.state_path.write_text(json.dumps(state))

    second = Executor(tmp_path / "home", server)
    second.apply({"cancel": [{"jobId": 1, "attempt": 1}]}, gpus())  # must not signal this test process
    second.attempts["1:1"].pop("termAt", None)
    report = settle(second, 1)

    assert report["state"] == "FAILED" and "vanished" in report["reason"]
    first.apply({"cancel": [{"jobId": 1, "attempt": 1}]}, gpus())
    settle(first, 1)


def test_forget_removes_the_attempt_and_its_directory_but_keeps_the_generation(executor):
    executor.apply({"run": [run(1, python("pass"))]}, gpus())
    settle(executor, 1)
    directory = executor.jobs_dir / "1-1"
    assert directory.exists()

    executor.apply({"forget": [{"jobId": 1, "attempt": 1}]}, gpus())

    assert executor.report() == [] and not directory.exists()
    assert json.loads(executor.state_path.read_text())["generations"] == {"1": 1}


def test_a_finished_attempt_waits_for_the_control_plane_before_it_is_reported(executor, server):
    server.down = True
    executor.apply({"run": [run(1, python("print('done')"))]}, gpus())
    time.sleep(1.0)
    executor.poll()
    executor.upload_logs()
    assert executor.report()[0]["state"] == "RUNNING"
    assert executor.attempts["1:1"]["state"] == "SUCCEEDED"

    server.down = False
    assert settle(executor, 1, timeout=20)["state"] == "SUCCEEDED"
    assert server.log(1) == "done\n"


def test_output_files_are_uploaded_with_checksums_and_symlinks_are_not_followed(executor, server, tmp_path):
    secret = tmp_path / "secret.txt"
    secret.write_text("not an output")
    code = ("import os, pathlib; out = pathlib.Path(os.environ['HYRAX_OUTPUT_DIR']); (out / 'run').mkdir(); "
            "(out / 'run' / 'metrics.json').write_text('{\"loss\": 0.5}'); (out / 'model.pt').write_bytes(b'weights'); "
            f"(out / 'link').symlink_to({str(secret)!r}); (out / 'refused.bin').write_bytes(b'x')")
    executor.apply({"run": [run(1, python(code))]}, gpus())

    report = settle(executor, 1)

    assert report["state"] == "SUCCEEDED"
    assert server.outputs == {"run/metrics.json": b'{"loss": 0.5}', "model.pt": b"weights"}
    assert "1 output file(s) were not stored: refused.bin" in report["reason"]


def packed(tmp_path, files):
    root = tmp_path / "project"
    for name, text in files.items():
        (root / name).parent.mkdir(parents=True, exist_ok=True)
        (root / name).write_text(text)
    archive = tmp_path / "snapshot.tar.gz"
    return archive, snapshot.pack(root, sorted(files), archive)


def test_runs_from_the_snapshot_in_the_submitted_directory_with_dataset_paths(executor, server, tmp_path):
    archive, sha256 = packed(tmp_path, {"lib/helper.py": "VALUE = 41\n", "experiments/train.py":
                                        "import os, sys; sys.path.insert(0, '../lib'); import helper; "
                                        "print(helper.VALUE + 1, os.environ['HYRAX_DATASET_TCGA_BRCA'])\n"})
    server.snapshots[9] = archive

    executor.apply({"run": [run(1, [sys.executable, "train.py"], snapshot={"id": 9, "sha256": sha256},
                                workdir="experiments", environment="none", datasets={"tcga-brca": "/data/tcga"})]}, gpus())
    report = settle(executor, 1)

    assert report["state"] == "SUCCEEDED", server.log(1)
    assert server.log(1) == "42 /data/tcga\n"


def test_a_snapshot_that_does_not_match_its_checksum_fails_the_attempt_without_running(executor, server, tmp_path):
    archive, _ = packed(tmp_path, {"train.py": "print('tampered')\n"})
    server.snapshots[9] = archive

    executor.apply({"run": [run(1, [sys.executable, "train.py"], snapshot={"id": 9, "sha256": "0" * 64})]}, gpus())
    report = settle(executor, 1)

    assert report["state"] == "FAILED" and "checksum" in report["reason"]
    assert "tampered" not in server.log(1)


@pytest.mark.skipif(shutil.which("uv") is None, reason="needs uv")
def test_uv_environment_is_built_from_the_lockfile_and_a_stale_lockfile_stops_the_job(executor, server, tmp_path):
    import subprocess
    project = tmp_path / "locked"
    project.mkdir()
    (project / "pyproject.toml").write_text('[project]\nname = "locked"\nversion = "0"\nrequires-python = ">=3.10"\ndependencies = []\n')
    subprocess.run(["uv", "lock", "--offline", "--project", str(project)], check=True, capture_output=True)
    (project / "where.py").write_text("import sys; print('venv' in sys.prefix, sys.prefix.endswith('1-1/venv'))\n")
    archive = tmp_path / "ok.tar.gz"
    sha256 = snapshot.pack(project, ["pyproject.toml", "uv.lock", "where.py"], archive)
    server.snapshots[1] = archive

    executor.apply({"run": [run(1, ["python", "where.py"], snapshot={"id": 1, "sha256": sha256}, environment="uv")]}, gpus())
    report = settle(executor, 1, timeout=120)
    assert report["state"] == "SUCCEEDED", server.log(1)
    assert "hyrax: environment ready in" in server.log(1)
    assert server.log(1).endswith("True True\n")

    # pyproject.toml gains a dependency the lockfile does not have: --locked must refuse, not re-resolve.
    (project / "pyproject.toml").write_text('[project]\nname = "locked"\nversion = "0"\nrequires-python = ">=3.10"\ndependencies = ["requests"]\n')
    stale = tmp_path / "stale.tar.gz"
    server.snapshots[2] = stale
    executor.apply({"run": [run(2, ["python", "where.py"], snapshot={
        "id": 2, "sha256": snapshot.pack(project, ["pyproject.toml", "uv.lock", "where.py"], stale)}, environment="uv")]}, gpus())
    report = settle(executor, 2, timeout=120)
    assert report["state"] == "FAILED" and "environment setup failed" in report["reason"]
    assert "the command was not run" in server.log(2)
