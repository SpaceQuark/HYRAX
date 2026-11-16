"""hyrax: the CLI."""

import argparse
import json
import os
import socket
import sys
import tempfile
import time
import urllib.parse
from datetime import datetime, timezone
from pathlib import Path

from . import snapshot
from .client import ApiError, Client, settings

ACTIVE = ("QUEUED", "ASSIGNED", "RUNNING", "LOST")


def megabytes(text: str) -> int:
    """'24G' or '500M' or a bare number of MiB."""
    units = {"M": 1, "G": 1024}
    unit = text[-1].upper()
    try:
        return int(float(text[:-1]) * units[unit]) if unit in units else int(text)
    except ValueError:
        raise argparse.ArgumentTypeError(f"not a memory size: {text} (try 24G)") from None


def status(client: Client, args) -> int:
    nodes = client.json("GET", "/api/nodes")
    if not nodes:
        print("No nodes have reported yet.")
        return 0
    for node in nodes:
        print(f"{node['node']}  {'online' if node['online'] else 'OFFLINE'}")
        for gpu in node["gpus"]:
            utilization = "?" if gpu["utilization"] is None else gpu["utilization"]
            reserved = "  reserved" if (node.get("flags") or {}).get(gpu["uuid"], {}).get("reserved") else ""
            print(
                f"  [{gpu['index']}] {gpu['name']:<28} {utilization:>3}%  "
                f"{gpu['memoryUsedMb']:>6} / {gpu['memoryTotalMb']} MiB  "
                f"{gpu['temperatureC']}°C{reserved}"
            )
            for proc in gpu["processes"]:
                memory = "?" if proc["memoryMb"] is None else proc["memoryMb"]
                command = (proc["command"] or "?")[:60]
                print(f"        {proc['pid']:>7} {proc['user'] or '?':<10} {memory:>6} MiB  {command}")
    return 0


def run(client: Client, args) -> int:
    command = args.command[1:] if args.command[:1] == ["--"] else args.command
    if not command:
        print("hyrax run: give a command, e.g. hyrax run --vram 20G -- python train.py", file=sys.stderr)
        return 2
    env = {}
    for pair in args.env:
        key, separator, value = pair.partition("=")
        if not separator:
            print(f"hyrax run: --env takes KEY=VALUE, not {pair}", file=sys.stderr)
            return 2
        env[key] = value
    node = args.node
    if args.local:
        node = settings().get("HYRAX_NODE") or socket.gethostname()
    try:
        source = None if args.no_snapshot else upload_snapshot(client, Path.cwd(), args.max_file_mb * 1024 * 1024)
    except snapshot.SnapshotError as error:
        print(f"hyrax run: {error}", file=sys.stderr)
        return 2
    environment = "none"
    if source and source["uv"] and not args.no_env:
        environment = "uv"
        print("Environment: uv sync --locked in a virtual environment made for this job.", file=sys.stderr)
    job = client.json("POST", "/api/jobs", {
        "name": args.name, "command": command, "env": env,
        "vramMb": args.vram, "gpuModel": args.gpu_model, "node": node,
        "snapshotId": source and source["id"], "workdir": source["workdir"] if source else ".",
        "environment": environment,
        "preferredLabels": [f"dataset:{name}" for name in args.dataset],
        "requiredLabels": [f"dataset:{name}" for name in args.require_dataset],
    })
    where = f"pinned to {node}" if node else "any node with a suitable free GPU"
    print(f"Submitted job {job['id']} ({where}).", file=sys.stderr)
    if not args.follow:
        return 0
    return follow(client, job["id"])


def upload_snapshot(client: Client, cwd: Path, max_file_bytes: int) -> dict | None:
    """Packs the project around `cwd` and makes sure the server has it. None when there is no project."""
    root = snapshot.find_root(cwd)
    if root is None:
        print("No source snapshot: this is neither a git repository nor a directory with a pyproject.toml. "
              "The command must already exist on the node that runs it.", file=sys.stderr)
        return None
    files, skipped = snapshot.collect(root, max_file_bytes)
    commit, dirty = snapshot.git_state(root)
    with tempfile.TemporaryDirectory() as scratch:
        archive = Path(scratch) / "snapshot.tar.gz"
        sha256 = snapshot.pack(root, files, archive)
        size = archive.stat().st_size
        try:
            found = client.json("GET", f"/api/snapshots/{sha256}")
            where = "already on the server"
        except ApiError as error:
            if error.status != 404:
                raise
            query = urllib.parse.urlencode({"gitDirty": "true" if dirty else "false", **({"gitCommit": commit} if commit else {})})
            found = client.upload(f"/api/snapshots/{sha256}?{query}", archive)
            where = "uploaded"
    revision = "not a git repository" if commit is None else f"commit {commit[:10]}" + (" plus uncommitted changes" if dirty else "")
    print(f"Snapshot {sha256[:12]}: {len(files)} files from {root}, {size / 1024:.0f} KiB, {revision} ({where}).", file=sys.stderr)
    for path, why in skipped[:8]:
        print(f"  left out: {path} ({why})", file=sys.stderr)
    if len(skipped) > 8:
        print(f"  left out: {len(skipped) - 8} more", file=sys.stderr)
    workdir = cwd.resolve().relative_to(root.resolve()).as_posix()
    # uv looks for the project upwards from where the command runs, so the lockfile may be in a parent.
    project = next((d for d in [cwd.resolve(), *cwd.resolve().parents] if (d / "uv.lock").exists()
                    and (d / "pyproject.toml").exists() and (d == root.resolve() or root.resolve() in d.parents)), None)
    locked = project is not None and project.relative_to(root.resolve()).joinpath("uv.lock").as_posix() in files
    return {"id": found["id"], "workdir": workdir, "uv": locked}


def age(timestamp: str | None) -> str:
    if not timestamp:
        return "-"
    seconds = int((datetime.now(timezone.utc) - datetime.fromisoformat(timestamp.replace("Z", "+00:00"))).total_seconds())
    for size, unit in ((86400, "d"), (3600, "h"), (60, "m")):
        if seconds >= size:
            return f"{seconds // size}{unit}"
    return f"{seconds}s"


def ps(client: Client, args) -> int:
    if args.job is not None:
        return describe(client.json("GET", f"/api/jobs/{args.job}"))
    jobs = client.json("GET", f"/api/jobs?all={'true' if args.all else 'false'}")
    if not jobs:
        print("No jobs." if args.all else "No queued, running or recently finished jobs.")
        return 0
    print(f"{'ID':>5}  {'USER':<10} {'STATE':<10} {'NODE':<14} {'GPU':<5} {'AGE':>4}  COMMAND")
    for job in jobs:
        gpus = ",".join(str(index) for index in job["gpuIndexes"]) or "-"
        state = job["state"] + ("*" if job["cancelRequested"] and job["state"] in ACTIVE else "")
        print(f"{job['id']:>5}  {job['user']:<10} {state:<10} {job['node'] or '-':<14} {gpus:<5} "
              f"{age(job['submittedAt']):>4}  {' '.join(job['command'])[:70]}")
    return 0


def describe(job: dict) -> int:
    print(f"job {job['id']}  {job['state']}  by {job['user']}  submitted {age(job['submittedAt'])} ago")
    print(f"  command: {' '.join(job['command'])}")
    wants = [f"{job['gpus']} GPU(s)"]
    if job["vramMb"]:
        wants.append(f"{job['vramMb']} MiB each")
    if job["gpuModel"]:
        wants.append(f"model {job['gpuModel']}")
    wants.append(f"on {job['wantedNode']}" if job["wantedNode"] else "on any node")
    if job["requiredLabels"]:
        wants.append("needs " + ", ".join(job["requiredLabels"]))
    if job["preferredLabels"]:
        wants.append("prefers " + ", ".join(job["preferredLabels"]))
    print(f"  wants:   {', '.join(wants)}")
    if job["snapshotSha256"]:
        revision = "no git commit" if not job["gitCommit"] else (
            f"commit {job['gitCommit'][:10]}" + (" plus uncommitted changes" if job["gitDirty"] else ""))
        print(f"  source:  snapshot {job['snapshotSha256'][:12]}, {revision}, run in {job['workdir']}"
              + (", uv environment" if job["environment"] == "uv" else ""))
    if job["attempt"]:
        gpus = ",".join(str(index) for index in job["gpuIndexes"])
        print(f"  attempt: {job['attempt']} on {job['node']} GPU {gpus}"
              + (f", exit {job['exitCode']}" if job["exitCode"] is not None else ""))
    if job["reason"]:
        print(f"  reason:  {job['reason']}")
    if job["lastDecision"]:
        print("  last scheduling decision:")
        for entry in job["lastDecision"]:
            gpu = f" GPU {entry['gpu']}" if "gpu" in entry else ""
            verdict = "chosen" if entry.get("chosen") else entry.get("rejected", "eligible, not needed")
            if entry.get("note"):
                verdict += f" ({entry['note']})"
            print(f"    {entry['node']}{gpu}: {verdict}")
    return 0


def follow(client: Client, job_id: int, wait: bool = True) -> int:
    """Prints the job's output. With `wait`, keeps going until the job ends and returns its exit code."""
    offset, attempt, announced = 0, None, None
    while True:
        data, headers = client.request("GET", f"/api/jobs/{job_id}/logs?offset={offset}")
        state = headers["X-Hyrax-Job-State"]
        if attempt is not None and headers["X-Hyrax-Attempt"] != attempt:
            # A new attempt starts a new log.
            attempt, offset = headers["X-Hyrax-Attempt"], 0
            continue
        attempt = headers["X-Hyrax-Attempt"]
        if data:
            offset = int(headers["X-Hyrax-Next-Offset"])
            sys.stdout.buffer.write(data)
            sys.stdout.buffer.flush()
            continue
        if not wait or state not in ACTIVE:
            break
        if state != announced and state != "RUNNING":
            print(f"[job {job_id} is {state}]", file=sys.stderr)
        announced = state
        time.sleep(1.0)
    if not wait:
        return 0
    job = client.json("GET", f"/api/jobs/{job_id}")
    print(f"[job {job_id} {job['state']}" + (f": {job['reason']}" if job["reason"] else "") + "]", file=sys.stderr)
    if job["state"] == "SUCCEEDED":
        return 0
    return job["exitCode"] if job["exitCode"] and job["exitCode"] > 0 else 1


def fetch(client: Client, args) -> int:
    """Downloads what a job left behind: its output files, checked against the manifest, its log and its record."""
    job = client.json("GET", f"/api/jobs/{args.job}")
    manifest = client.json("GET", f"/api/jobs/{args.job}/artifacts")
    if job["state"] in ACTIVE:
        print(f"hyrax fetch: job {args.job} is {job['state']}; its outputs are uploaded when it ends.", file=sys.stderr)
        return 1
    target = Path(args.output or f"hyrax-job-{args.job}")
    outputs = target / "outputs"
    outputs.mkdir(parents=True, exist_ok=True)
    bad, total = [], 0
    for entry in manifest["files"]:
        try:
            # The manifest comes over the network: a path in it is never trusted to stay inside the target.
            destination = snapshot.safe_join(outputs, entry["path"])
        except snapshot.SnapshotError as error:
            bad.append(str(error))
            continue
        destination.parent.mkdir(parents=True, exist_ok=True)
        partial = destination.with_name(destination.name + ".part")
        arrived = client.download(f"/api/jobs/{args.job}/artifacts/file?{urllib.parse.urlencode({'path': entry['path']})}", partial)
        if arrived != entry["sha256"]:
            partial.unlink()
            bad.append(f"{entry['path']}: checksum mismatch")
            continue
        os.replace(partial, destination)
        total += entry["sizeBytes"]
    with open(target / "output.log", "wb") as log:
        offset = 0
        while True:
            data, headers = client.request("GET", f"/api/jobs/{args.job}/logs?offset={offset}")
            if not data:
                break
            log.write(data)
            offset = int(headers["X-Hyrax-Next-Offset"])
    (target / "job.json").write_text(json.dumps({"job": job, "outputs": manifest["files"]}, indent=2) + "\n")
    print(f"Job {args.job} ({job['state']}): {len(manifest['files']) - len(bad)} output file(s), "
          f"{total / (1024 * 1024):.1f} MiB, verified, in {outputs}/. Log and record in {target}/.")
    for problem in bad:
        print(f"hyrax fetch: {problem}", file=sys.stderr)
    return 1 if bad else 0


def labels(client: Client, args) -> int:
    found = client.json("GET", "/api/labels")
    if not found:
        print("No labels. An admin adds one with: hyrax admin label <node> dataset:<name>=/path/on/that/node")
    for entry in found:
        print(f"{entry['node']:<14} {entry['label']:<28} {entry['path'] or ''}")
    return 0


def logs(client: Client, args) -> int:
    return follow(client, args.job, wait=args.follow)


def cancel(client: Client, args) -> int:
    job = client.json("POST", f"/api/jobs/{args.job}/cancel")
    if job["state"] in ACTIVE:
        print(f"Job {job['id']} is {job['state']} on {job['node']}; its agent has been asked to stop it.")
    else:
        print(f"Job {job['id']} is {job['state']}.")
    return 0


def admin(client: Client, args) -> int:
    if args.action in ("label", "unlabel"):
        if not args.label:
            print(f"hyrax admin {args.action}: give a label, e.g. dataset:camelyon16=/data/camelyon16", file=sys.stderr)
            return 2
        label, _, path = args.label.partition("=")
        if args.action == "label":
            client.json("PUT", f"/api/admin/nodes/{args.name}/labels", {"label": label, "path": path or None})
        else:
            client.request("DELETE", f"/api/admin/nodes/{args.name}/labels?{urllib.parse.urlencode({'label': label})}")
        return 0
    if args.action == "add-user":
        created = client.json("POST", "/api/admin/users", {"name": args.name, "admin": args.admin})
    else:
        created = client.json("POST", f"/api/admin/nodes/{args.name}/enroll")
    # The token is shown once; only its hash is kept.
    print(created["token"])
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(prog="hyrax")
    parser.add_argument("--server", help="control plane URL (default: HYRAX_SERVER)")
    commands = parser.add_subparsers(dest="command_name", required=True)

    commands.add_parser("status", help="show every node, GPU and process").set_defaults(handler=status)

    run_parser = commands.add_parser("run", help="queue a command to run on a free GPU, on any node by default")
    run_parser.add_argument("--vram", type=megabytes, metavar="SIZE", help="GPU memory needed, e.g. 20G")
    run_parser.add_argument("--gpu-model", metavar="TEXT", help="only GPUs whose name contains this, e.g. 4090")
    where = run_parser.add_mutually_exclusive_group()
    where.add_argument("--node", help="run on this node only")
    where.add_argument("--local", action="store_true", help="run on this machine only")
    run_parser.add_argument("--name")
    run_parser.add_argument("--dataset", action="append", default=[], metavar="NAME",
                            help="prefer a node that has this dataset; its path arrives in HYRAX_DATASET_<NAME>")
    run_parser.add_argument("--require-dataset", action="append", default=[], metavar="NAME",
                            help="only run on a node that has this dataset")
    run_parser.add_argument("--no-snapshot", action="store_true",
                            help="do not send this project's source; the command must exist on the node")
    run_parser.add_argument("--no-env", action="store_true", help="do not build a uv environment from uv.lock")
    run_parser.add_argument("--max-file-mb", type=int, default=50, metavar="MB",
                            help="leave files larger than this out of the snapshot (default 50)")
    run_parser.add_argument("--env", action="append", default=[], metavar="KEY=VALUE")
    run_parser.add_argument("-f", "--follow", action="store_true", help="print the output and wait for the job")
    run_parser.add_argument("command", nargs=argparse.REMAINDER, help="the command, after --")
    run_parser.set_defaults(handler=run)

    ps_parser = commands.add_parser("ps", help="list jobs, or explain one job")
    ps_parser.add_argument("job", nargs="?", type=int)
    ps_parser.add_argument("-a", "--all", action="store_true", help="include jobs that finished over a day ago")
    ps_parser.set_defaults(handler=ps)

    logs_parser = commands.add_parser("logs", help="print a job's output")
    logs_parser.add_argument("job", type=int)
    logs_parser.add_argument("-f", "--follow", action="store_true", help="keep printing until the job ends")
    logs_parser.set_defaults(handler=logs)

    fetch_parser = commands.add_parser("fetch", help="download a finished job's output files, log and record")
    fetch_parser.add_argument("job", type=int)
    fetch_parser.add_argument("-o", "--output", metavar="DIR", help="where to put them (default hyrax-job-<id>)")
    fetch_parser.set_defaults(handler=fetch)

    commands.add_parser("labels", help="list what each node has locally, such as datasets").set_defaults(handler=labels)

    cancel_parser = commands.add_parser("cancel", help="stop a job")
    cancel_parser.add_argument("job", type=int)
    cancel_parser.set_defaults(handler=cancel)

    admin_parser = commands.add_parser("admin", help="create users, enroll nodes (both print the new token), label nodes")
    admin_parser.add_argument("action", choices=["add-user", "enroll", "label", "unlabel"])
    admin_parser.add_argument("name", help="user name, or node name")
    admin_parser.add_argument("label", nargs="?", help="with label/unlabel: dataset:<name>[=/path/on/the/node]")
    admin_parser.add_argument("--admin", action="store_true", help="with add-user: make the user an admin")
    admin_parser.set_defaults(handler=admin)

    args = parser.parse_args()
    config = settings()
    client = Client(args.server or config["HYRAX_SERVER"], config.get("HYRAX_TOKEN"), config.get("HYRAX_CA"))
    try:
        sys.exit(args.handler(client, args))
    except ApiError as error:
        hint = "  (set HYRAX_TOKEN to your personal API token)" if error.status == 401 else ""
        print(f"hyrax: {error}{hint}", file=sys.stderr)
        sys.exit(1)
    except OSError as error:
        print(f"hyrax: cannot reach {client.server}: {error}", file=sys.stderr)
        sys.exit(1)
    except KeyboardInterrupt:
        sys.exit(130)


if __name__ == "__main__":
    main()
