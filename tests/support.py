"""Helpers that tests/e2e and tests/upstream share: starting emulators and the server."""

import json
import os
import shutil
import socket
import subprocess
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
BINARY = ROOT / "target" / "debug" / "lakekeeper-firestore"
FIRESTORE_IMAGE = "gcr.io/google.com/cloudsdktool/google-cloud-cli:emulators"
# How long an emulator or the server gets to come up.
STARTUP_TIMEOUT = 300


def http(method: str, url: str, body: dict | None = None) -> bytes:
    data = None if body is None else json.dumps(body).encode()
    request = urllib.request.Request(url, data=data, method=method)
    if data is not None:
        request.add_header("content-type", "application/json")
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            return response.read()
    except urllib.error.HTTPError as e:
        raise RuntimeError(f"{method} {url}: {e.code} {e.read().decode(errors='replace')}") from e


def http_ok(url: str) -> bool:
    try:
        with urllib.request.urlopen(url, timeout=5) as response:
            return response.status == 200
    except (urllib.error.URLError, OSError):
        return False


def port_open(port: int) -> bool:
    with socket.socket() as s:
        s.settimeout(1)
        return s.connect_ex(("localhost", port)) == 0


def wait_until(what: str, ready, process: subprocess.Popen | None = None, log: Path | None = None):
    deadline = time.monotonic() + STARTUP_TIMEOUT
    while time.monotonic() < deadline:
        if process is not None and process.poll() is not None:
            output = log.read_text() if log is not None else ""
            raise RuntimeError(f"{what} exited with {process.returncode}\n{output}")
        if ready():
            return
        time.sleep(1)
    raise RuntimeError(f"{what} did not come up within {STARTUP_TIMEOUT} s")


def docker(*args: str, check: bool = True) -> str:
    return subprocess.run(
        ["docker", *args], check=check, capture_output=True, text=True
    ).stdout.strip()


def start_firestore(name: str, port: int):
    """Start the Firestore emulator in container `name` unless it is running."""
    if not docker("ps", "-q", "-f", f"name=^{name}$"):
        docker("rm", "-f", name, check=False)
        docker(
            "run", "-d", "--name", name, "-p", f"{port}:8080", FIRESTORE_IMAGE,
            "gcloud", "emulators", "firestore", "start", "--host-port=0.0.0.0:8080",
        )
    wait_until("The Firestore emulator", lambda: http_ok(f"http://localhost:{port}/"))


def wipe_firestore(port: int, project: str):
    http(
        "DELETE",
        f"http://localhost:{port}/emulator/v1/projects/{project}/databases/(default)/documents",
    )


def build():
    subprocess.run(["cargo", "build", "--quiet"], cwd=ROOT, check=True)


class Server:
    """The server from target/debug, with `env` on top of this process's environment."""

    def __init__(self, port: int, env: dict[str, str], log: Path):
        self.url = f"http://localhost:{port}"
        self.env = env
        self.log = log
        self.process = subprocess.Popen(
            [str(BINARY), "serve"],
            env={**os.environ, "LAKEKEEPER__LISTEN_PORT": str(port), **env},
            stdout=log.open("w"),
            stderr=subprocess.STDOUT,
        )
        try:
            wait_until("The server", lambda: http_ok(f"{self.url}/health"), self.process, log)
        except BaseException:
            self.stop()
            raise

    def bootstrap(self):
        http("POST", f"{self.url}/management/v1/bootstrap", {"accept-terms-of-use": True})

    def logged_errors(self) -> list[str]:
        return [line for line in self.log.read_text().splitlines() if '"level":"ERROR"' in line]

    def stop(self):
        stop(self.process)


def stop(process: subprocess.Popen):
    process.terminate()
    try:
        process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        process.kill()


def keep_log(log: Path):
    """Copy `log` to $KEEP_LOG, if set."""
    keep = os.environ.get("KEEP_LOG")
    if keep and log.exists():
        shutil.copy(log, keep)
