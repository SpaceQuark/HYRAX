import io
import subprocess
import tarfile

import pytest

from hyrax import snapshot


def git(root, *args):
    subprocess.run(["git", "-C", str(root), "-c", "user.name=t", "-c", "user.email=t@example.com", *args],
                   check=True, capture_output=True)


@pytest.fixture
def project(tmp_path):
    root = tmp_path / "project"
    (root / "src").mkdir(parents=True)
    (root / "src" / "train.py").write_text("print('hi')\n")
    (root / "pyproject.toml").write_text("[project]\nname='p'\nversion='0'\n")
    (root / ".gitignore").write_text("ignored/\n*.ckpt\n")
    git(root, "init", "-q")
    git(root, "add", "-A")
    git(root, "commit", "-qm", "first")
    return root


def test_takes_tracked_and_untracked_files_but_not_ignored_ones(project):
    (project / "new.py").write_text("x = 1\n")
    (project / "model.ckpt").write_bytes(b"0" * 10)
    (project / "ignored").mkdir()
    (project / "ignored" / "data.bin").write_bytes(b"0" * 10)

    files, skipped = snapshot.collect(project)

    assert files == [".gitignore", "new.py", "pyproject.toml", "src/train.py"]
    assert skipped == []


def test_leaves_out_credentials_large_files_environments_and_outside_links(project, tmp_path):
    (project / ".env").write_text("TOKEN=secret\n")
    (project / "deploy.pem").write_text("-----BEGIN PRIVATE KEY-----\n")
    (project / "big.npy").write_bytes(b"0" * 2048)
    (project / ".venv" / "bin").mkdir(parents=True)
    (project / ".venv" / "bin" / "python").write_text("")
    (tmp_path / "outside.txt").write_text("not yours\n")
    (project / "escape").symlink_to(tmp_path / "outside.txt")
    (project / "inside").symlink_to("src/train.py")
    (project / ".hyraxignore").write_text("# comment\nscratch\n")
    (project / "scratch").mkdir()
    (project / "scratch" / "notes.txt").write_text("x")

    files, skipped = snapshot.collect(project, max_file_bytes=1024)

    assert "inside" in files
    assert not {".env", "deploy.pem", "big.npy", "escape", ".venv/bin/python", "scratch/notes.txt"} & set(files)
    reasons = dict(skipped)
    assert reasons[".env"] == "looks like a credential"
    assert reasons["deploy.pem"] == "looks like a credential"
    assert "larger than" in reasons["big.npy"]
    assert reasons["escape"] == "symlink pointing outside the project"
    assert reasons["scratch/notes.txt"] == "matches .hyraxignore"


def test_git_state_reports_commit_and_uncommitted_changes(project):
    commit, dirty = snapshot.git_state(project)
    assert len(commit) == 40 and not dirty
    (project / "src" / "train.py").write_text("print('changed')\n")
    assert snapshot.git_state(project) == (commit, True)


def test_the_same_tree_packs_to_the_same_bytes_and_round_trips(project, tmp_path):
    files, _ = snapshot.collect(project)
    first = snapshot.pack(project, files, tmp_path / "a.tar.gz")
    (project / "src" / "train.py").touch()
    assert snapshot.pack(project, files, tmp_path / "b.tar.gz") == first

    snapshot.extract(tmp_path / "a.tar.gz", tmp_path / "out")
    assert (tmp_path / "out" / "src" / "train.py").read_text() == "print('hi')\n"

    (project / "src" / "train.py").write_text("print('changed')\n")
    assert snapshot.pack(project, files, tmp_path / "c.tar.gz") != first


def test_a_directory_without_git_or_pyproject_is_not_a_project(tmp_path):
    assert snapshot.find_root(tmp_path) is None
    (tmp_path / "pyproject.toml").write_text("")
    assert snapshot.find_root(tmp_path) == tmp_path


def test_pack_refuses_an_oversized_snapshot(project, tmp_path):
    files, _ = snapshot.collect(project)
    with pytest.raises(snapshot.SnapshotError, match="over the"):
        snapshot.pack(project, files, tmp_path / "a.tar.gz", max_bytes=10)


@pytest.mark.parametrize("name", ["../escape.txt", "/tmp/hyrax-absolute.txt"])
def test_extract_refuses_archives_that_write_outside(tmp_path, name):
    archive = tmp_path / "evil.tar.gz"
    with tarfile.open(archive, "w:gz") as tar:
        info = tarfile.TarInfo(name)
        info.size = 4
        tar.addfile(info, io.BytesIO(b"evil"))
    snapshot.extract(archive, tmp_path / "out" / "deep") if name.startswith("/") else None
    if name.startswith("/"):
        # The data filter strips the leading slash, so the file lands inside the destination.
        assert (tmp_path / "out" / "deep" / "tmp" / "hyrax-absolute.txt").exists()
    else:
        with pytest.raises(snapshot.SnapshotError):
            snapshot.extract(archive, tmp_path / "out" / "deep")
        assert not (tmp_path / "out" / "escape.txt").exists()


@pytest.mark.parametrize("path", ["../x", "/etc/passwd", "a/../../x", "", "a//b", "a\\b", "."])
def test_safe_join_refuses_paths_that_leave_the_base(tmp_path, path):
    with pytest.raises(snapshot.SnapshotError):
        snapshot.safe_join(tmp_path, path)


def test_safe_join_accepts_nested_relative_paths(tmp_path):
    assert snapshot.safe_join(tmp_path, "run/ckpt/model.pt") == tmp_path.resolve() / "run" / "ckpt" / "model.pt"
