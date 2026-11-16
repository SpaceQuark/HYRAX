"""HTTP to the control plane, for both the agent and the CLI. Standard library only."""

import hashlib
import json
import os
import ssl
import urllib.error
import urllib.request
from pathlib import Path

from . import DEFAULT_SERVER

CONFIG_DIR = Path.home() / ".config" / "hyrax"


class ApiError(Exception):
    def __init__(self, status: int, message: str):
        super().__init__(message)
        self.status = status


def settings() -> dict:
    """HYRAX_* settings: agent.env, then cli.env, then the environment, later ones winning."""
    values = {}
    for name in ("agent.env", "cli.env"):
        try:
            lines = (CONFIG_DIR / name).read_text().splitlines()
        except OSError:
            continue
        for line in lines:
            key, separator, value = line.strip().partition("=")
            if separator and key.startswith("HYRAX_"):
                values[key] = value
    values.update({key: value for key, value in os.environ.items() if key.startswith("HYRAX_")})
    values.setdefault("HYRAX_SERVER", DEFAULT_SERVER)
    if "HYRAX_CA" not in values and (CONFIG_DIR / "ca.pem").exists():
        values["HYRAX_CA"] = str(CONFIG_DIR / "ca.pem")
    return values


class Client:
    def __init__(self, server: str, token: str | None = None, ca: str | None = None):
        self.server = server.rstrip("/")
        self.token = token
        self.secure = self.server.startswith("https://")
        # The control plane's certificate is signed by the lab's own CA, so that is the only one trusted.
        self.context = ssl.create_default_context(cafile=ca) if self.secure else None

    def request(self, method: str, path: str, body: bytes | None = None,
                content_type: str = "application/json", timeout: float = 10.0):
        """Returns (body bytes, headers). Raises ApiError for an HTTP error, OSError when unreachable."""
        request = urllib.request.Request(f"{self.server}{path}", data=body, method=method)
        if body is not None:
            request.add_header("Content-Type", content_type)
        if self.token:
            request.add_header("Authorization", f"Bearer {self.token}")
        try:
            with urllib.request.urlopen(request, timeout=timeout, context=self.context) as response:
                return response.read(), response.headers
        except urllib.error.HTTPError as error:
            detail = error.read()
            try:
                message = json.loads(detail).get("message") or error.reason
            except (ValueError, AttributeError):
                message = error.reason
            raise ApiError(error.code, f"{error.code} {message}") from None

    def _open(self, request, timeout: float):
        if self.token:
            request.add_header("Authorization", f"Bearer {self.token}")
        try:
            return urllib.request.urlopen(request, timeout=timeout, context=self.context)
        except urllib.error.HTTPError as error:
            detail = error.read()
            try:
                message = json.loads(detail).get("message") or error.reason
            except (ValueError, AttributeError):
                message = error.reason
            raise ApiError(error.code, f"{error.code} {message}") from None

    def upload(self, path: str, file_path, timeout: float = 120.0):
        """PUTs a file without reading it into memory. Returns the parsed reply."""
        with open(file_path, "rb") as f:
            request = urllib.request.Request(f"{self.server}{path}", data=f, method="PUT")
            request.add_header("Content-Type", "application/octet-stream")
            request.add_header("Content-Length", str(os.fstat(f.fileno()).st_size))
            with self._open(request, timeout) as response:
                return json.loads(response.read())

    def download(self, path: str, file_path, timeout: float = 120.0) -> str:
        """GETs into a file without holding it in memory. Returns the sha256 of what arrived."""
        digest = hashlib.sha256()
        with self._open(urllib.request.Request(f"{self.server}{path}"), timeout) as response, open(file_path, "wb") as f:
            for block in iter(lambda: response.read(1 << 20), b""):
                digest.update(block)
                f.write(block)
        return digest.hexdigest()

    def json(self, method: str, path: str, body=None, timeout: float = 10.0):
        data, _ = self.request(method, path, None if body is None else json.dumps(body).encode(), timeout=timeout)
        return json.loads(data) if data else None
