"""Smoke test of the container image against the Firestore emulator.

    docker build -t lakekeeper-firestore:dev .
    IMAGE=lakekeeper-firestore:dev uv run --project tests pytest tests/image

Skipped unless IMAGE names an image. The image runs in the network namespace of its own
emulator container (lkfs-firestore-image, removed afterwards), which publishes both ports, so
the server reaches the emulator at localhost and the test reaches the server through it.
"""

import json
import os
import subprocess
import urllib.error
import urllib.request

import pytest

import support

IMAGE = os.environ.get("IMAGE")
FIRESTORE_PORT = int(os.environ.get("FIRESTORE_PORT", "28083"))
CATALOG_PORT = int(os.environ.get("CATALOG_PORT", "18184"))
EMULATOR = "lkfs-firestore-image"
SERVER = "lkfs-image-server"

pytestmark = pytest.mark.skipif(not IMAGE, reason="IMAGE is not set")


def run_image(*args: str, detach: bool = False) -> str:
    return support.docker(
        "run", *(["-d", "--name", SERVER] if detach else ["--rm"]),
        "--network", f"container:{EMULATOR}",
        "-e", "FIRESTORE_EMULATOR_HOST=localhost:8080",
        "-e", "LAKEKEEPER__FIRESTORE__PROJECT_ID=demo-lakekeeper",
        IMAGE, *args,
    )


@pytest.fixture(scope="module")
def url():
    for name in (SERVER, EMULATOR):
        support.docker("rm", "-f", name, check=False)
    support.docker(
        "run", "-d", "--name", EMULATOR,
        "-p", f"{FIRESTORE_PORT}:8080", "-p", f"{CATALOG_PORT}:8181",
        support.FIRESTORE_IMAGE,
        "gcloud", "emulators", "firestore", "start", "--host-port=0.0.0.0:8080",
    )
    try:
        support.wait_until(
            "The Firestore emulator", lambda: support.http_ok(f"http://localhost:{FIRESTORE_PORT}/")
        )
        run_image(detach=True)
        url = f"http://localhost:{CATALOG_PORT}"

        def ready():
            if support.docker("ps", "-q", "-f", f"name=^{SERVER}$") == "":
                raise RuntimeError("the server exited:\n" + support.docker("logs", SERVER, check=False))
            return support.http_ok(f"{url}/health")

        support.wait_until("The server", ready)
        yield url
    finally:
        for name in (SERVER, EMULATOR):
            support.docker("rm", "-f", name, check=False)


def test_version():
    assert run_image("version") == "0.1.0"


def test_health(url):
    with urllib.request.urlopen(f"{url}/health") as response:
        assert json.load(response)["health"] == "ok"


def test_healthcheck_subcommand(url):
    # As a container health probe would run it, against the server and Firestore.
    out = subprocess.run(
        ["docker", "exec", SERVER, "/home/nonroot/lakekeeper-firestore", "healthcheck", "-a"],
        capture_output=True,
        text=True,
    )
    assert out.returncode == 0, out.stdout + out.stderr


def test_bootstrap_ignores_an_authorization_header(url):
    # Clients behind Cloud Run IAM send a Google ID token, which this server must not refuse.
    headers = {"Authorization": "Bearer not-a-token", "content-type": "application/json"}
    request = urllib.request.Request(
        f"{url}/management/v1/bootstrap",
        data=json.dumps({"accept-terms-of-use": True}).encode(),
        headers=headers,
        method="POST",
    )
    with urllib.request.urlopen(request) as response:
        assert response.status in (200, 204)
    info = urllib.request.Request(f"{url}/management/v1/info", headers=headers)
    with urllib.request.urlopen(info) as response:
        assert json.load(response)["bootstrapped"] is True


def test_console_is_served_when_built_in(url):
    try:
        with urllib.request.urlopen(f"{url}/ui/") as response:
            body = response.read()
    except urllib.error.HTTPError as e:
        if e.code == 404:
            pytest.skip("built without the console (FEATURES=ui)")
        raise
    assert b"<html" in body.lower()
