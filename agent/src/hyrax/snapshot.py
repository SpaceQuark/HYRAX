"""Source snapshots: the project tree a job runs from, packed on the submitting machine.

In a git repository the snapshot is what git would track (tracked files plus untracked ones that are not
ignored), as they are on disk, so uncommitted changes travel too. On top of that, some things never go:
version-control and environment directories, files that look like credentials, and large files.
"""

import fnmatch
import gzip
import hashlib
import os
import subprocess
import tarfile
from pathlib import Path

EXCLUDED_DIRS = {
    ".git", ".hg", ".svn", ".venv", "venv", "node_modules", "__pycache__", ".mypy_cache", ".pytest_cache",
    ".ruff_cache", ".tox", ".ipynb_checkpoints", "wandb", ".hyrax",
}
CREDENTIALS = [
    ".env", ".env.*", "*.pem", "*.key", "*.p12", "*.pfx", "*.keystore", "*.kdbx", "id_rsa*", "id_ed25519*",
    "id_ecdsa*", ".netrc", ".pypirc", ".npmrc", "credentials", "credentials.*", "secrets.*", "*.tfstate",
]
MAX_FILE_BYTES = 50 * 1024 * 1024
MAX_SNAPSHOT_BYTES = 200 * 1024 * 1024


class SnapshotError(Exception):
    pass


def _git(root: Path, *args: str) -> str | None:
    try:
        done = subprocess.run(["git", "-C", str(root), *args], capture_output=True, check=True)
    except (OSError, subprocess.CalledProcessError):
        return None
    return done.stdout.decode(errors="surrogateescape")


def find_root(cwd: Path) -> Path | None:
    """The project to snapshot: the git repository around `cwd`, else `cwd` itself if it is a Python project."""
    top = _git(cwd, "rev-parse", "--show-toplevel")
    if top:
        return Path(top.strip())
    return cwd if (cwd / "pyproject.toml").exists() else None


def git_state(root: Path) -> tuple[str | None, bool]:
    """(commit, dirty). Dirty means the snapshot holds changes that the commit does not."""
    commit = _git(root, "rev-parse", "HEAD")
    status = _git(root, "status", "--porcelain")
    return (commit.strip() if commit else None), bool(status and status.strip())


def collect(root: Path, max_file_bytes: int = MAX_FILE_BYTES) -> tuple[list[str], list[tuple[str, str]]]:
    """Returns (paths to include, [(path, why it was left out)]), both relative to `root` and sorted."""
    listed = _git(root, "ls-files", "-z", "--cached", "--others", "--exclude-standard")
    if listed is not None:
        candidates = [path for path in listed.split("\0") if path]
    else:
        candidates = []
        for directory, names, files in os.walk(root):
            names[:] = [name for name in names if name not in EXCLUDED_DIRS]
            candidates += [os.path.relpath(os.path.join(directory, name), root) for name in files]
    try:
        ignore = [line.strip() for line in (root / ".hyraxignore").read_text().splitlines()]
        ignore = [line for line in ignore if line and not line.startswith("#")]
    except OSError:
        ignore = []

    included, skipped = [], []
    for relative in sorted(set(candidates)):
        parts = Path(relative).parts
        path = root / relative
        name = parts[-1]
        if any(part in EXCLUDED_DIRS or part.endswith(".egg-info") for part in parts[:-1]):
            continue  # Whole directories; listing every file in them would bury the interesting skips.
        if any(fnmatch.fnmatch(relative, pattern) or fnmatch.fnmatch(name, pattern)
               or fnmatch.fnmatch(relative, pattern.rstrip("/") + "/*") for pattern in ignore):
            skipped.append((relative, "matches .hyraxignore"))
        elif any(fnmatch.fnmatch(name, pattern) for pattern in CREDENTIALS):
            skipped.append((relative, "looks like a credential"))
        elif path.is_symlink():
            target = (path.parent / os.readlink(path)).resolve()
            if root.resolve() in target.parents:
                included.append(relative)
            else:
                skipped.append((relative, "symlink pointing outside the project"))
        elif not path.is_file():
            continue  # Deleted but still in the index, or a submodule.
        elif path.stat().st_size > max_file_bytes:
            skipped.append((relative, f"larger than {max_file_bytes // (1024 * 1024)} MB"))
        else:
            included.append(relative)
    return included, skipped


def pack(root: Path, files: list[str], archive: Path, max_bytes: int = MAX_SNAPSHOT_BYTES) -> str:
    """Writes a .tar.gz of `files` and returns its sha256. The same tree always gives the same bytes,
    so the server can tell it already has a snapshot."""
    with open(archive, "wb") as raw, gzip.GzipFile(filename="", fileobj=raw, mode="wb", mtime=0) as zipped, \
            tarfile.open(fileobj=zipped, mode="w", format=tarfile.PAX_FORMAT) as tar:
        for relative in files:
            path = root / relative
            info = tar.gettarinfo(str(path), arcname=relative)
            info.uid = info.gid = 0
            info.uname = info.gname = ""
            info.mtime = 0
            info.pax_headers = {}
            if info.isfile():
                info.mode = 0o755 if info.mode & 0o100 else 0o644
                with open(path, "rb") as f:
                    tar.addfile(info, f)
            else:
                tar.addfile(info)
    if archive.stat().st_size > max_bytes:
        raise SnapshotError(
            f"the snapshot is {archive.stat().st_size // (1024 * 1024)} MB, over the {max_bytes // (1024 * 1024)} MB limit; "
            "add large or generated paths to .gitignore or .hyraxignore")
    return sha256_of(archive)


def sha256_of(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def extract(archive: Path, destination: Path) -> None:
    """Unpacks a snapshot. The 'data' filter refuses absolute paths, '..', links out of the tree and devices."""
    if not hasattr(tarfile, "data_filter"):
        raise SnapshotError("this Python cannot unpack archives safely; it needs 3.10.12, 3.11.4 or newer")
    destination.mkdir(parents=True, exist_ok=True)
    try:
        with tarfile.open(archive, "r:gz") as tar:
            tar.extractall(destination, filter="data")
    except (tarfile.TarError, OSError) as error:
        raise SnapshotError(f"could not unpack the snapshot: {error}") from None


def safe_join(base: Path, relative: str) -> Path:
    """`base/relative`, or SnapshotError if that would be anywhere but under `base`."""
    parts = relative.split("/")
    if not relative or relative.startswith("/") or "\\" in relative or any(part in ("", ".", "..") for part in parts):
        raise SnapshotError(f"unsafe path {relative!r}")
    path = (base / relative).resolve()
    if base.resolve() not in path.parents:
        raise SnapshotError(f"unsafe path {relative!r}")
    return path
